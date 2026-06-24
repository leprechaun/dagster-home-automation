import polars as pl
from dagster import (
    AutomationCondition,
    AssetCheckResult,
    AssetExecutionContext,
    Definitions,
    EnvVar,
    asset,
    asset_check,
)

@asset(
    group_name="home_automation",
    io_manager_key="home_automation_io_manager",
    key_prefix=["home-automation", "bronze"],
    automation_condition=AutomationCondition.eager(),
)
def power_by_area_and_date(context: AssetExecutionContext) -> pl.DataFrame:
    storage_options = {
        "endpoint_url": EnvVar("AWS_ENDPOINT_URL_S3").get_value(),
    }
    df = pl.read_delta(
        "s3://deltalake/home-automation/electricity/",
        storage_options=storage_options,
    ).group_by([
        pl.col("timestamp").dt.date(),
        "area"
    ]).agg(
        pl.col("power").min().alias("min"),
        pl.col("power").mean().alias("mean"),
        pl.col("power").max().alias("max"),
        pl.col("power").quantile(0.95).alias("p95"),
        pl.col("power").quantile(0.5).alias("p50"),
        pl.col("power").quantile(0.05).alias("p05"),
    )
    context.log.info(f"Read {len(df)} rows from electricity delta table")
    return df

@asset_check(asset=power_by_area_and_date, blocking=True)
def power_by_area_and_date_not_empty(power_by_area_and_date: pl.DataFrame) -> AssetCheckResult:
    row_count = len(power_by_area_and_date)
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": row_count},
    )

defs = Definitions(
    assets=[power_by_area_and_date],
    asset_checks=[power_by_area_and_date_not_empty],
)
