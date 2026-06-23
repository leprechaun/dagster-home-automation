import boto3

import dagster as dg
from dagster import Definitions, EnvVar, InputContext, OutputContext
from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager


_s3_config = S3Config(allow_unsafe_rename=True, endpoint=EnvVar("AWS_ENDPOINT_URL_S3"))

defs = Definitions(resources={
    "home_automation_bronze_io_manager": DeltaLakePolarsIOManager(
        root_uri="s3://deltalake/",
        storage_options=_s3_config,
        schema="home-automation-bronze",
    )
})
