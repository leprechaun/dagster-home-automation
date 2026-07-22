import polars as pl
from dagster import (
    AutomationCondition,
    AssetCheckResult,
    AssetExecutionContext,
    AssetSpec,
    Definitions,
    AssetKey,
    AssetIn,
    asset,
    asset_check,
)

huami_extended_activity_sample = AssetSpec(
    key=["gadgetbridge", "bronze", "huami_extended_activity_sample"],
).with_io_manager_key("gadgetbridge_io_manager")

@asset(
    group_name="home_automation",
    io_manager_key="home_automation_io_manager",
    key_prefix=["home-automation", "silver"],
    automation_condition=AutomationCondition.eager(),
    ins = {
        "electricity": AssetIn(key=AssetKey(["home-automation", "raw", "electricity"])),
        "activity": AssetIn(key=AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
    }
)
def power_and_activity_by_minute(context: AssetExecutionContext, electricity: pl.LazyFrame, activity: pl.LazyFrame) -> pl.DataFrame:
    power_by_minute = electricity.group_by(
        pl.col("timestamp").dt.truncate("1m").alias("minute")
    ).agg(
        pl.col("power").mean().alias("avg_power"),
    )

    activity_by_minute = activity.group_by(
        pl.col("TIMESTAMP").dt.truncate("1m").alias("minute")
    ).agg(
        pl.col("RAW_INTENSITY").mean().alias("avg_raw_intensity"),
    )

    df = power_by_minute.join(
        activity_by_minute, on="minute", how="full", coalesce=True
    ).sort("minute").collect()

    context.log.info(f"Joined {len(df)} minute buckets of power and activity data")
    return df

@asset_check(asset=power_and_activity_by_minute, blocking=True)
def power_and_activity_by_minute_not_empty(power_and_activity_by_minute: pl.DataFrame) -> AssetCheckResult:
    row_count = len(power_and_activity_by_minute)
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": row_count},
    )

defs = Definitions(
    assets=[huami_extended_activity_sample, power_and_activity_by_minute],
    asset_checks=[power_and_activity_by_minute_not_empty],
)
