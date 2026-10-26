from datetime import datetime, timezone

import polars as pl
from dagster import build_asset_context

from dagster_home_automation.defs.assets.silver_home_assistant import (
    area_versions,
    device_versions,
    entity_versions,
)

CREATED = datetime(2026, 9, 1, tzinfo=timezone.utc)
FIRMWARE_UPDATED = datetime(2026, 10, 1, 1, 30, tzinfo=timezone.utc)


def raw(rows: list[dict]) -> pl.LazyFrame:
    # Shaped like the raw registry tables: snapshot_time as ISO text,
    # created_at/modified_at as epoch seconds.
    return pl.DataFrame(rows, infer_schema_length=None).lazy()


def raw_device(hour: int, **attributes) -> dict:
    row = {
        "id": "dev1",
        "snapshot_time": f"2026-10-01T{hour:02d}:00:00+00:00",
        "created_at": CREATED.timestamp(),
        "modified_at": CREATED.timestamp(),
        "config_entry_id": "cfg1",
        "area_id": "office",
        "name": "Smart Plug",
        "name_by_user": "Fan plug",
        "manufacturer": "Acme",
        "model": "Plug V3",
        "hw_version": "1.0",
        "sw_version": "2.0",
        "disabled_by": None,
        "via_device_id": None,
        "labels": [],
    }
    row.update(attributes)
    return row


def test_device_firmware_update_starts_a_new_device_version_keyed_by_device_id():
    devices = raw(
        [
            raw_device(0),
            raw_device(1, config_entry_id="cfg2"),
            raw_device(2, sw_version="2.1", modified_at=FIRMWARE_UPDATED.timestamp()),
        ]
    )

    versions = device_versions(build_asset_context(), devices).sort("valid_from")

    assert versions.select("device_id", "sw_version", "valid_from", "valid_to").to_dicts() == [
        {"device_id": "dev1", "sw_version": "2.0", "valid_from": CREATED, "valid_to": FIRMWARE_UPDATED},
        {"device_id": "dev1", "sw_version": "2.1", "valid_from": FIRMWARE_UPDATED, "valid_to": None},
    ]


def test_device_versions_keep_tracked_attributes_only():
    versions = device_versions(build_asset_context(), raw([raw_device(0)]))

    assert versions.columns == [
        "device_id", "valid_from", "valid_to", "last_seen", "is_removed",
        "area_id", "name", "name_by_user", "manufacturer", "model", "hw_version", "sw_version",
        "disabled_by", "via_device_id", "labels",
    ]


def test_entity_versions_keep_tracked_attributes_only():
    entities = raw(
        [
            {
                "entity_id": "light.fan",
                "snapshot_time": "2026-10-01T00:00:00+00:00",
                "created_at": CREATED.timestamp(),
                "modified_at": CREATED.timestamp(),
                "config_entry_id": "cfg1",
                "device_id": "dev1",
                "area_id": None,
                "name": None,
                "original_name": "Fan",
                "platform": "tasmota",
                "disabled_by": None,
                "hidden_by": None,
                "entity_category": None,
            }
        ]
    )

    versions = entity_versions(build_asset_context(), entities)

    assert versions.columns == [
        "entity_id", "valid_from", "valid_to", "last_seen", "is_removed",
        "device_id", "area_id", "name", "original_name", "platform", "disabled_by", "hidden_by",
    ]


def test_area_versions_keep_tracked_attributes_only():
    areas = raw(
        [
            {
                "area_id": "office",
                "snapshot_time": "2026-10-01T00:00:00+00:00",
                "created_at": CREATED.timestamp(),
                "modified_at": CREATED.timestamp(),
                "name": "Office",
                "floor_id": "ground",
                "labels": [],
                "icon": "mdi:desk",
                "aliases": [],
            }
        ]
    )

    versions = area_versions(build_asset_context(), areas)

    assert versions.columns == [
        "area_id", "valid_from", "valid_to", "last_seen", "is_removed",
        "name", "floor_id", "labels",
    ]
