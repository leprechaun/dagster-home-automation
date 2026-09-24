import asyncio
from datetime import datetime

from dagster import ConfigurableResource, Definitions, EnvVar
from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager
from dagster_delta import ClientConfig as HomeAssistantClientConfig
from dagster_delta import DeltaLakePolarsIOManager as HomeAssistantDeltaLakePolarsIOManager
from dagster_delta import S3Config as HomeAssistantS3Config
from dagster_openlineage import openlineage_sensor

from dagster_home_automation.home_assistant.client import (
    fetch_all_entity_ids,
    fetch_history,
    fetch_registries,
    rest_url,
    websocket_url,
)


class HomeAssistantResource(ConfigurableResource):
    url: str
    token: str

    def fetch_registries(self, commands: list[str]) -> dict[str, list[dict]]:
        return asyncio.run(fetch_registries(websocket_url(self.url), self.token, commands))

    def fetch_history(self, start: datetime, end: datetime) -> list[dict]:
        base_url = rest_url(self.url)
        entities = fetch_all_entity_ids(base_url, self.token)
        return fetch_history(base_url, self.token, start, end, entities)


_s3_config = S3Config(allow_unsafe_rename=True, endpoint=EnvVar("AWS_ENDPOINT_URL_S3"))

# dagster-deltalake's write-time partition-overwrite predicate is broken for
# TimeWindowPartitionsDefinition (see docs/dagster-deltalake-timestamp-partition-bug.md),
# which entity_history relies on. dagster-delta (ASML-Labs fork) has this fixed and
# tested, so it's used here instead — scoped to just this IO manager, since none of
# our other assets are partitioned and therefore never hit the bug.
_home_assistant_s3_config = HomeAssistantS3Config(endpoint=EnvVar("AWS_ENDPOINT_URL_S3"))
_home_assistant_client_config = HomeAssistantClientConfig(MOUNT_ALLOW_UNSAFE_RENAME="true")

defs = Definitions(
    sensors=[openlineage_sensor(include_asset_events=True)],
    resources={
        "home_automation_io_manager": DeltaLakePolarsIOManager(
            root_uri="s3://deltalake/home-automation/",
            storage_options=_s3_config,
        ),
        "home_assistant_io_manager": HomeAssistantDeltaLakePolarsIOManager(
            root_uri="s3://deltalake/home-assistant/",
            storage_options=_home_assistant_s3_config,
            client_options=_home_assistant_client_config,
        ),
        "gadgetbridge_io_manager": DeltaLakePolarsIOManager(
            root_uri="s3://deltalake/gadgetbridge/",
            storage_options=_s3_config,
        ),
        "hass": HomeAssistantResource(
            url=EnvVar("HASS_URL"),
            token=EnvVar("HASS_TOKEN"),
        ),
    }
)
