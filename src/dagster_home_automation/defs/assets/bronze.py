import polars as pl
from dagster import AutomationCondition, Definitions, AssetExecutionContext, asset, EnvVar

@asset(
    group_name="home_automation_bronze",
    io_manager_key="home_automation_bronze_io_manager",
    key_prefix="home_automation_bronze",
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

defs = Definitions(assets=[power_by_area_and_date])
