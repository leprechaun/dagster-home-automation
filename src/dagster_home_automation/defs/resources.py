
from dagster import Definitions, EnvVar
from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager
from dagster_openlineage import openlineage_sensor


_s3_config = S3Config(allow_unsafe_rename=True, endpoint=EnvVar("AWS_ENDPOINT_URL_S3"))

defs = Definitions(
    sensors=[openlineage_sensor(include_asset_events=True)],
    resources={
        "home_automation_io_manager": DeltaLakePolarsIOManager(
            root_uri="s3://deltalake/home-automation/",
            storage_options=_s3_config,
        )
    }
)
