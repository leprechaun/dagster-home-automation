import asyncio
import os
from datetime import datetime

from dagster import ConfigurableResource, Definitions, EnvVar, OutputContext
from dagster_apprise import AppriseNotificationsConfig, apprise_notifications
from dagster_deltalake import DeltaLakePyArrowTypeHandler, S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager, DeltaLakePolarsTypeHandler
from dagster_delta import ClientConfig as HomeAssistantClientConfig
from dagster_delta import DeltaLakePolarsIOManager as HomeAssistantDeltaLakePolarsIOManager
from dagster_delta import S3Config as HomeAssistantS3Config
from dagster_delta.io_manager.arrow import _DeltaLakePyArrowTypeHandler as HomeAssistantPyArrowTypeHandler
from dagster_delta.io_manager.polars import _DeltaLakePolarsTypeHandler as HomeAssistantPolarsTypeHandler
from dagster_openlineage import openlineage_sensor
from deltalake import DeltaTable

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


def sync_table_description(dt: DeltaTable, description: str | None) -> bool:
    """Set the table's Delta-level description, returning whether it changed.

    Each alter is its own commit in the Delta log, so only write when the
    description actually differs.
    """
    if not description or dt.metadata().description == description:
        return False
    dt.alter.set_table_description(description)
    return True


class _SyncsDescription:
    """Type handler mixin: after writing, copy the asset's description onto its
    Delta table, so readers outside Dagster (e.g. the marimo notebooks) see it.
    Works for both dagster-deltalake and dagster-delta, whose TableConnections
    share table_uri/storage_options.
    """

    def handle_output(self, context: OutputContext, table_slice, obj, connection):
        super().handle_output(context, table_slice, obj, connection)
        dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)
        if sync_table_description(dt, context.asset_spec.description):
            context.log.info(f"Updated Delta table description on {connection.table_uri}")


class DescribedDeltaLakePolarsTypeHandler(_SyncsDescription, DeltaLakePolarsTypeHandler):
    pass


class DescribedDeltaLakePolarsIOManager(DeltaLakePolarsIOManager):
    """DeltaLakePolarsIOManager that also syncs asset descriptions onto tables."""

    @staticmethod
    def type_handlers():
        return [DescribedDeltaLakePolarsTypeHandler(), DeltaLakePyArrowTypeHandler()]


class DescribedHomeAssistantPolarsTypeHandler(_SyncsDescription, HomeAssistantPolarsTypeHandler):
    pass


class DescribedHomeAssistantDeltaLakePolarsIOManager(HomeAssistantDeltaLakePolarsIOManager):
    """dagster-delta's DeltaLakePolarsIOManager, also syncing asset descriptions onto tables."""

    @staticmethod
    def type_handlers():
        return [DescribedHomeAssistantPolarsTypeHandler(), HomeAssistantPyArrowTypeHandler()]


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
        "home_automation_io_manager": DescribedDeltaLakePolarsIOManager(
            root_uri="s3://deltalake/home-automation/",
            storage_options=_s3_config,
        ),
        "home_assistant_io_manager": DescribedHomeAssistantDeltaLakePolarsIOManager(
            root_uri="s3://deltalake/home-assistant/",
            storage_options=_home_assistant_s3_config,
            client_options=_home_assistant_client_config,
        ),
        "gadgetbridge_io_manager": DescribedDeltaLakePolarsIOManager(
            root_uri="s3://deltalake/gadgetbridge/",
            storage_options=_s3_config,
        ),
        "hass": HomeAssistantResource(
            url=EnvVar("HASS_URL"),
            token=EnvVar("HASS_TOKEN"),
        ),
    }
)

_apprise_url = os.environ.get("APPRISE_NOTIFICATION_URL")
if _apprise_url:
    defs = Definitions.merge(
        defs,
        apprise_notifications(
            AppriseNotificationsConfig(
                urls=[_apprise_url],
                events=["FAILURE"],
                title_prefix="Home Automation Pipeline",
            )
        ),
    )
