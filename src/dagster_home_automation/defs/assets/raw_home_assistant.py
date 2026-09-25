import json
from datetime import datetime, timezone

import polars as pl
from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    Definitions,
    HourlyPartitionsDefinition,
    asset,
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


def _registry_asset(name: str, command: str):
    @asset(
        name=name,
        key_prefix=["home-assistant", "raw"],
        group_name="home_automation",
        io_manager_key="home_assistant_io_manager",
        automation_condition=AutomationCondition.on_cron("0 * * * *"),
        metadata={"mode": "append"},
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

    return _asset


areas = _registry_asset("areas", "config/area_registry/list")
devices = _registry_asset("devices", "config/device_registry/list")
entities = _registry_asset("entities", "config/entity_registry/list")

# Adjust to whenever you actually want state history backfilled from —
# a Home Assistant instance's recorder retention is typically only ~10 days,
# so there's no point starting this further back than that.
_ENTITY_HISTORY_PARTITIONS = HourlyPartitionsDefinition(start_date="2026-09-14-00:00")


@asset(
    key_prefix=["home-assistant", "raw"],
    group_name="home_automation",
    io_manager_key="home_assistant_io_manager",
    partitions_def=_ENTITY_HISTORY_PARTITIONS,
    metadata={"partition_expr": "partition_hour"},
    pool="home_assistant_api",
)
def entity_history(context: AssetExecutionContext, hass: HomeAssistantResource) -> pl.DataFrame:
    start, end = context.partition_time_window
    rows = hass.fetch_history(start, end)

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
        pl.lit(start).alias("partition_hour"),
    )

    # HA's history API also returns each entity's carried-forward state as of
    # the window start, for entities that didn't change during the window —
    # stamped with last_changed == last_updated == the query's start_time
    # itself, not the entity's true prior change time. Those exactly-on-the-
    # boundary rows fail the partition_expr overwrite predicate (delta-rs
    # requires every written row to fall strictly within the replaced
    # partition), so use a strictly-open lower bound to drop them — the real
    # change was already captured in whichever partition it actually
    # happened in.
    before_filter = len(df)
    df = df.filter(pl.col("last_updated").is_between(start, end, closed="none"))
    dropped = before_filter - len(df)

    context.log.info(
        f"Fetched {len(rows)} state rows for {start}..{end}, dropped {dropped} carried-forward rows; schema={df.schema}"
    )
    return df


defs = Definitions(assets=[areas, devices, entities, entity_history])
