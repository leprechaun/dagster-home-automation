import json
from datetime import datetime, timezone

import polars as pl
from dagster import (
    AssetCheckResult,
    AutomationCondition,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    asset,
    asset_check,
)

from dagster_home_automation.defs.resources import HomeAssistantResource

# Fields that are always populated numerics in the HA registries, so we pin
# their dtype rather than let an all-null batch infer them as pl.Null too.
_TIMESTAMP_FIELDS = ("created_at", "modified_at")


def _normalize_row(row: dict, extra: dict | None = None) -> dict:
    # Nested values (e.g. entity "categories"/"options", state "attributes")
    # have a shape that varies per integration, so a Struct column would
    # drift schema between snapshots. Flatten them to JSON text instead of
    # letting Polars infer a struct.
    row = {key: (json.dumps(value) if isinstance(value, dict) else value) for key, value in row.items()}
    if extra:
        row.update(extra)
    return row


def _widen_null_columns(df: pl.DataFrame) -> pl.DataFrame:
    # A column that's None in every row of this batch (e.g. area_id before
    # anything's been assigned to an area) infers as pl.Null, which Delta
    # can't store. Every nullable field in these registries is string-valued
    # when present, so Utf8 is a safe, stable fallback across snapshots.
    casts = []
    for column, dtype in df.schema.items():
        if dtype == pl.Null:
            casts.append(pl.col(column).cast(pl.Utf8))
        elif dtype == pl.List(pl.Null):
            casts.append(pl.col(column).cast(pl.List(pl.Utf8)))
    return df.with_columns(casts) if casts else df


def _registry_asset(name: str, command: str, required_columns: tuple[str, ...]):
    @asset(
        name=name,
        key_prefix=["home-assistant", "raw"],
        group_name="home_automation",
        io_manager_key="home_assistant_io_manager",
        automation_condition=AutomationCondition.on_cron("0 * * * *"),
        # schema_mode="merge": HA adds fields to its registry APIs over
        # versions (e.g. next_name_part appearing on device registry
        # entries) — without this, append fails outright the moment a
        # snapshot has more fields than the table's existing schema. merge
        # tolerates a field disappearing from a batch too (backfills null),
        # which is fine for fields we don't use but would be a silent,
        # undetected correctness problem for ones we depend on downstream
        # (see the required_columns check below).
        metadata={"mode": "append", "schema_mode": "merge"},
        pool="home_assistant_api",
    )
    def _asset(context: AssetExecutionContext, hass: HomeAssistantResource) -> pl.DataFrame:
        rows = hass.fetch_registries([command])[command]
        snapshot_time = datetime.now(timezone.utc).isoformat()

        normalized = [_normalize_row(row, {"snapshot_time": snapshot_time}) for row in rows]
        schema_overrides = {field: pl.Float64 for field in _TIMESTAMP_FIELDS}
        df = pl.DataFrame(normalized, schema_overrides=schema_overrides, infer_schema_length=None)
        df = _widen_null_columns(df)

        context.log.info(f"Fetched {len(rows)} rows from {command}; schema={df.schema}")
        return df

    @asset_check(asset=_asset, name=f"{name}_has_required_columns", blocking=True)
    def _has_required_columns(df: pl.DataFrame) -> AssetCheckResult:
        missing = sorted(set(required_columns) - set(df.columns))
        return AssetCheckResult(
            passed=not missing,
            metadata={"missing_columns": missing},
        )

    return _asset, _has_required_columns


# Required columns are exactly what entity_history_enriched's join_asof
# chain depends on downstream (see silver_home_assistant.py) — not every
# field we happen to receive, since harmless fields coming and going (like
# next_name_part) shouldn't trip this.
areas, areas_has_required_columns = _registry_asset(
    "areas", "config/area_registry/list", required_columns=("area_id", "name")
)
devices, devices_has_required_columns = _registry_asset(
    "devices", "config/device_registry/list", required_columns=("id", "area_id", "name", "name_by_user")
)
entities, entities_has_required_columns = _registry_asset(
    "entities",
    "config/entity_registry/list",
    required_columns=("entity_id", "device_id", "area_id", "name", "platform"),
)

# Adjust to whenever you actually want state history backfilled from —
# a Home Assistant instance's recorder retention is typically only ~10 days,
# so there's no point starting this further back than that.
#
# end_offset=1 makes today count as a valid, materializable partition
# (otherwise a partition's window has to fully close before Dagster will
# offer it at all, which is why data was landing a day late). The asset
# body already handles an in-progress "today" correctly — it fetches
# [day_start, now) and overwrites, so re-running it repeatedly through the
# day is safe.
_ENTITY_HISTORY_PARTITIONS = DailyPartitionsDefinition(start_date="2026-09-15", end_offset=1)


@asset(
    key_prefix=["home-assistant", "raw"],
    group_name="home_automation",
    io_manager_key="home_assistant_io_manager",
    partitions_def=_ENTITY_HISTORY_PARTITIONS,
    metadata={"partition_expr": "partition_day"},
    automation_condition=AutomationCondition.on_cron("0 * * * *"),
    pool="home_assistant_api",
)
def entity_history(context: AssetExecutionContext, hass: HomeAssistantResource) -> pl.DataFrame:
    # Physical partitioning is daily (see partition_day below — dagster_delta
    # validates every written row against a predicate built from this
    # asset's own partition window regardless of write mode, so the physical
    # column has to be constant across that whole window; hourly physical
    # partitions would fail validation for every hour except midnight).
    #
    # Always fetch the whole day so far (not just an incremental slice) and
    # overwrite (the default mode) rather than append. This makes every run
    # a complete, idempotent snapshot of the day up to now — a missed cron
    # tick (e.g. the daemon being down for a few hours) is invisible on the
    # next run, since that run just re-fetches from midnight again rather
    # than trusting a fixed lookback window to have covered the gap.
    day_start, day_end = context.partition_time_window
    now = datetime.now(timezone.utc)
    fetch_start, fetch_end = day_start, min(day_end, now)

    rows = hass.fetch_history(fetch_start, fetch_end)

    normalized = []
    for row in rows:
        row = dict(row)
        row.setdefault("last_updated", row.get("last_changed"))
        normalized.append(_normalize_row(row))

    df = pl.DataFrame(normalized, infer_schema_length=None)
    df = _widen_null_columns(df)
    context.log.info(df.schema)
    context.log.info(df)
    df = df.with_columns(
        pl.col("last_changed").str.to_datetime(time_unit="us", time_zone="UTC"),
        pl.col("last_updated").str.to_datetime(time_unit="us", time_zone="UTC"),
        pl.lit(day_start.date()).alias("partition_day"),
    )

    # HA's history API also returns each entity's carried-forward state as of
    # the fetch window start (midnight), for entities that didn't change
    # since before then — stamped with last_changed == last_updated ==
    # day_start itself, not the entity's true prior change time. Drop those;
    # they don't represent a real event at that timestamp.
    before_filter = len(df)
    df = df.filter(pl.col("last_updated").is_between(fetch_start, fetch_end, closed="none"))
    dropped = before_filter - len(df)

    context.log.info(
        f"Fetched {len(rows)} state rows for {fetch_start}..{fetch_end}, dropped {dropped} carried-forward rows; schema={df.schema}"
    )
    return df


defs = Definitions(
    assets=[areas, devices, entities, entity_history],
    asset_checks=[areas_has_required_columns, devices_has_required_columns, entities_has_required_columns],
)
