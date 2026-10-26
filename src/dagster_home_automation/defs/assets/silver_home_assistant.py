import polars as pl
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AutomationCondition,
    Definitions,
    asset,
    asset_check,
)

from dagster_home_automation.defs.assets.raw_home_assistant import _ENTITY_HISTORY_PARTITIONS
from dagster_home_automation.home_assistant.versions import build_versions


def _with_parsed_snapshot_time(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(pl.col("snapshot_time").str.to_datetime(time_zone="UTC"))


def _versions_asset(name: str, key: str, tracked: list[str], raw_key: str | None = None):
    # Rebuilt in full from every raw snapshot on each run — see
    # docs/adr/0001 for why raw is kept complete to make that possible.
    @asset(
        name=name,
        key_prefix=["home-assistant", "silver"],
        group_name="home_automation",
        io_manager_key="home_assistant_io_manager",
        automation_condition=AutomationCondition.eager(),
        ins={"snapshots": AssetIn(key=AssetKey(["home-assistant", "raw", name]))},
    )
    def _asset(context: AssetExecutionContext, snapshots: pl.LazyFrame) -> pl.DataFrame:
        parsed = (
            _with_parsed_snapshot_time(snapshots)
            .rename({raw_key: key} if raw_key else {})
            .with_columns(
                pl.from_epoch(pl.col(c).cast(pl.Float64) * 1_000_000, time_unit="us").dt.replace_time_zone("UTC")
                for c in ("created_at", "modified_at")
            )
            .select(key, "snapshot_time", "created_at", "modified_at", *tracked)
            .collect()
        )
        versions = build_versions(parsed, key=key, tracked=tracked)
        context.log.info(f"Built {len(versions)} versions from {len(parsed)} snapshot rows")
        return versions

    # Blocking: entity_history_enriched picks the version in force by
    # valid_from alone, which is only right if versions are contiguous and
    # never overlap.
    @asset_check(asset=_asset, name=f"{name}_versions_are_well_formed", blocking=True)
    def _well_formed(versions: pl.DataFrame) -> AssetCheckResult:
        ordered = versions.sort(key, "valid_from")
        next_from = pl.col("valid_from").shift(-1).over(key)

        def keys_where(condition: pl.Expr) -> list[str]:
            return sorted(ordered.filter(condition)[key].unique().to_list())

        # A current version (valid_to null) followed by another is open-ended, so it overlaps too.
        broken = {
            "overlapping": keys_where(next_from.is_not_null() & (pl.col("valid_to").is_null() | (pl.col("valid_to") > next_from))),
            "not_exactly_one_current": keys_where(pl.col("valid_to").is_null().sum().over(key) != 1),
            "empty_or_backwards": keys_where(pl.col("valid_to") <= pl.col("valid_from")),
        }
        return AssetCheckResult(passed=not any(broken.values()), metadata=broken)

    return _asset, _well_formed


# Tracked attributes: only what matters for analysis — a change to anything
# else (config entries, identifiers, modified_at itself) doesn't start a new
# version.
entity_versions, entity_versions_are_well_formed = _versions_asset(
    "entities",
    key="entity_id",
    tracked=["device_id", "area_id", "name", "original_name", "platform", "disabled_by", "hidden_by"],
)
device_versions, device_versions_are_well_formed = _versions_asset(
    "devices",
    key="device_id",
    raw_key="id",
    tracked=[
        "area_id", "name", "name_by_user", "manufacturer", "model", "hw_version", "sw_version",
        "disabled_by", "via_device_id", "labels",
    ],
)
area_versions, area_versions_are_well_formed = _versions_asset("areas", key="area_id", tracked=["name", "floor_id", "labels"])


@asset(
    key_prefix=["home-assistant", "silver"],
    group_name="home_automation",
    io_manager_key="home_assistant_io_manager",
    partitions_def=_ENTITY_HISTORY_PARTITIONS,
    metadata={"partition_expr": "partition_day"},
    automation_condition=AutomationCondition.eager(),
    ins={
        "entity_history": AssetIn(key=AssetKey(["home-assistant", "raw", "entity_history"])),
        "entities": AssetIn(key=AssetKey(["home-assistant", "silver", "entities"])),
        "devices": AssetIn(key=AssetKey(["home-assistant", "silver", "devices"])),
        "areas": AssetIn(key=AssetKey(["home-assistant", "silver", "areas"])),
    },
)
def entity_history_enriched(
    context: AssetExecutionContext,
    entity_history: pl.LazyFrame,
    entities: pl.LazyFrame,
    devices: pl.LazyFrame,
    areas: pl.LazyFrame,
) -> pl.DataFrame:
    # device_class/unit_of_measurement are pulled directly as strings via
    # json_path_match rather than a full json_decode of attributes — that
    # column's shape varies by entity domain, and these two fields are the
    # only ones every numeric sensor shares consistently.
    history_prepped = (
        entity_history
        .with_columns(
            pl.col("attributes").str.json_path_match("$.device_class").alias("device_class"),
            pl.col("attributes").str.json_path_match("$.unit_of_measurement").alias("unit_of_measurement"),
            # The name HA itself displayed at the time. The registry's own
            # `name` is only a user override (null for most entities), and
            # rebuilding HA's naming rules from the registry would need
            # has_entity_name + device name + original_name.
            pl.col("attributes").str.json_path_match("$.friendly_name").alias("entity_name"),
            # state stays as-is (it's the correct representation for
            # categorical/boolean entities); value is the opportunistic
            # numeric cast, null wherever state isn't actually a number.
            pl.col("state").cast(pl.Float64, strict=False).alias("value"),
        )
        .select(
            "entity_id", "last_updated", "partition_day", "state", "value", "device_class", "unit_of_measurement", "entity_name"
        )
        .sort("entity_id", "last_updated")
    )

    entities_prepped = (
        entities
        .select(
            "entity_id",
            "valid_from",
            "device_id",
            pl.col("area_id").alias("entity_area_id"),
        )
        .sort("entity_id", "valid_from")
    )

    # name_by_user is what's actually shown in the HA UI when set, overriding
    # the integration-provided name — same precedence HA itself uses.
    devices_prepped = (
        devices
        .select(
            "device_id",
            "valid_from",
            pl.coalesce(["name_by_user", "name"]).alias("device_name"),
            pl.col("area_id").alias("device_area_id"),
        )
        .sort("device_id", "valid_from")
    )

    areas_prepped = (
        areas
        .select("area_id", "valid_from", pl.col("name").alias("area_name"))
        .sort("area_id", "valid_from")
    )

    # Each state-change event gets the entity/device/area version in force
    # at the moment it happened. Versions are contiguous and never overlap
    # (blocking checks on the silver registries), so the latest valid_from
    # at or before the event *is* that version — an asof join, which unlike
    # a join-then-filter on [valid_from, valid_to) keeps events no version
    # covers. Each join requires the frame re-sorted by its own (by-key,
    # time) pair first — the sort order from the previous join doesn't carry
    # over to the next join's grouping.
    history_with_entity = (
        history_prepped
        .join_asof(
            entities_prepped,
            left_on="last_updated",
            right_on="valid_from",
            by="entity_id",
            strategy="backward",
            check_sortedness=False,
        )
        .drop("valid_from")
    )

    history_with_device = (
        history_with_entity
        .sort("device_id", "last_updated")
        .join_asof(
            devices_prepped,
            left_on="last_updated",
            right_on="valid_from",
            by="device_id",
            strategy="backward",
            check_sortedness=False,
        )
        .drop("valid_from")
    )

    # Effective area: an entity's own area_id, if set, overrides its
    # device's — same precedence HA itself uses for area resolution.
    history_with_area_id = history_with_device.with_columns(
        pl.coalesce(["entity_area_id", "device_area_id"]).alias("area_id")
    )

    history_with_area = (
        history_with_area_id
        .sort("area_id", "last_updated")
        .join_asof(
            areas_prepped,
            left_on="last_updated",
            right_on="valid_from",
            by="area_id",
            strategy="backward",
            check_sortedness=False,
        )
    )

    df = history_with_area.select(
        "entity_id",
        "last_updated",
        "partition_day",
        "state",
        "value",
        "device_class",
        "unit_of_measurement",
        "device_id",
        "device_name",
        "area_id",
        "area_name",
        "entity_name",
    ).collect()

    context.log.info(f"Enriched {len(df)} state-change rows; schema={df.schema}")
    return df


@asset_check(asset=entity_history_enriched, blocking=True)
def entity_history_enriched_not_empty(entity_history_enriched: pl.DataFrame) -> AssetCheckResult:
    row_count = len(entity_history_enriched)
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": row_count},
    )


defs = Definitions(
    assets=[entity_versions, device_versions, area_versions, entity_history_enriched],
    asset_checks=[
        entity_versions_are_well_formed,
        device_versions_are_well_formed,
        area_versions_are_well_formed,
        entity_history_enriched_not_empty,
    ],
)
