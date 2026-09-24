import json
from datetime import datetime, timezone

import polars as pl
from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    Definitions,
    asset,
)

from dagster_home_automation.defs.resources import HomeAssistantResource

# Fields that are always populated numerics in the HA registries, so we pin
# their dtype rather than let an all-null batch infer them as pl.Null too.
_TIMESTAMP_FIELDS = ("created_at", "modified_at")


def _normalize_row(row: dict, snapshot_time: str) -> dict:
    # Nested values (e.g. entity "categories"/"options") have a shape that
    # varies per integration, so a Struct column would drift schema between
    # snapshots. Flatten them to JSON text instead of letting Polars infer a
    # struct.
    row = {key: (json.dumps(value) if isinstance(value, dict) else value) for key, value in row.items()}
    row["snapshot_time"] = snapshot_time
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
    )
    def _asset(context: AssetExecutionContext, hass: HomeAssistantResource) -> pl.DataFrame:
        rows = hass.fetch_registries([command])[command]
        snapshot_time = datetime.now(timezone.utc).isoformat()

        normalized = [_normalize_row(row, snapshot_time) for row in rows]
        schema_overrides = {field: pl.Float64 for field in _TIMESTAMP_FIELDS}
        df = pl.DataFrame(normalized, schema_overrides=schema_overrides, infer_schema_length=None)
        df = _widen_null_columns(df)

        context.log.info(f"Fetched {len(rows)} rows from {command}; schema={df.schema}")
        return df

    return _asset


areas = _registry_asset("areas", "config/area_registry/list")
devices = _registry_asset("devices", "config/device_registry/list")
entities = _registry_asset("entities", "config/entity_registry/list")

defs = Definitions(assets=[areas, devices, entities])
