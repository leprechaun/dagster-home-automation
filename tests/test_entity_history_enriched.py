from datetime import date, datetime, timezone

import polars as pl
from dagster import build_asset_context

from dagster_home_automation.defs.assets.silver_home_assistant import entity_history_enriched

UTC = pl.Datetime("us", "UTC")
BEGINNING = datetime(2026, 9, 1, tzinfo=timezone.utc)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc)


def events(*rows: tuple[str, datetime]) -> pl.LazyFrame:
    return pl.DataFrame(
        [
            {
                "entity_id": entity_id,
                "last_updated": when,
                "partition_day": date(2026, 10, 1),
                "state": "12.5",
                "attributes": '{"device_class": "power", "unit_of_measurement": "W"}',
            }
            for entity_id, when in rows
        ],
        schema={"entity_id": pl.Utf8, "last_updated": UTC, "partition_day": pl.Date, "state": pl.Utf8, "attributes": pl.Utf8},
    ).lazy()


def versions(key: str, rows: list[dict], attributes: dict[str, pl.DataType]) -> pl.LazyFrame:
    defaults = {"valid_to": None, "last_seen": at(23), "is_removed": False}
    return pl.DataFrame(
        [{**defaults, **row} for row in rows],
        schema={key: pl.Utf8, "valid_from": UTC, "valid_to": UTC, "last_seen": UTC, "is_removed": pl.Boolean, **attributes},
    ).lazy()


def entities(*rows: dict) -> pl.LazyFrame:
    return versions("entity_id", list(rows), {"device_id": pl.Utf8, "area_id": pl.Utf8, "name": pl.Utf8})


def devices(*rows: dict) -> pl.LazyFrame:
    return versions("device_id", list(rows), {"area_id": pl.Utf8, "name": pl.Utf8, "name_by_user": pl.Utf8})


def areas(*rows: dict) -> pl.LazyFrame:
    return versions("area_id", list(rows), {"name": pl.Utf8})


def enrich(history, entity_versions, device_versions, area_versions) -> list[dict]:
    df = entity_history_enriched(build_asset_context(), history, entity_versions, device_versions, area_versions)
    return df.sort("entity_id", "last_updated").to_dicts()


def test_event_gets_the_names_and_area_in_force_when_it_happened():
    rows = enrich(
        events(("sensor.fan_power", at(9)), ("sensor.fan_power", at(10)), ("sensor.fan_power", at(11))),
        entities(
            {"entity_id": "sensor.fan_power", "valid_from": BEGINNING, "valid_to": at(10), "device_id": "dev1", "area_id": "office", "name": "Fan power"},
            {"entity_id": "sensor.fan_power", "valid_from": at(10), "device_id": "dev1", "area_id": "kitchen", "name": "Fan power"},
        ),
        devices({"device_id": "dev1", "valid_from": BEGINNING, "name": "Smart Plug"}),
        areas(
            {"area_id": "office", "valid_from": BEGINNING, "name": "Office"},
            {"area_id": "kitchen", "valid_from": BEGINNING, "valid_to": at(10, 30), "name": "Kitchen"},
            {"area_id": "kitchen", "valid_from": at(10, 30), "name": "Kitchen & dining"},
        ),
    )

    assert [(r["last_updated"], r["area_id"], r["area_name"]) for r in rows] == [
        (at(9), "office", "Office"),
        (at(10), "kitchen", "Kitchen"),
        (at(11), "kitchen", "Kitchen & dining"),
    ]
    assert rows[0] | {"last_updated": None} == {
        "entity_id": "sensor.fan_power",
        "last_updated": None,
        "partition_day": date(2026, 10, 1),
        "state": "12.5",
        "value": 12.5,
        "device_class": "power",
        "unit_of_measurement": "W",
        "device_id": "dev1",
        "device_name": "Smart Plug",
        "area_id": "office",
        "area_name": "Office",
        "entity_name": "Fan power",
    }


def test_entity_without_its_own_area_is_in_its_devices_area():
    rows = enrich(
        events(("sensor.fan_power", at(9))),
        entities({"entity_id": "sensor.fan_power", "valid_from": BEGINNING, "device_id": "dev1", "area_id": None}),
        devices({"device_id": "dev1", "valid_from": BEGINNING, "area_id": "office"}),
        areas({"area_id": "office", "valid_from": BEGINNING, "name": "Office"}),
    )

    assert [(r["area_id"], r["area_name"]) for r in rows] == [("office", "Office")]


def test_entitys_own_area_overrides_its_devices_area():
    rows = enrich(
        events(("sensor.fan_power", at(9))),
        entities({"entity_id": "sensor.fan_power", "valid_from": BEGINNING, "device_id": "dev1", "area_id": "kitchen"}),
        devices({"device_id": "dev1", "valid_from": BEGINNING, "area_id": "office"}),
        areas(
            {"area_id": "office", "valid_from": BEGINNING, "name": "Office"},
            {"area_id": "kitchen", "valid_from": BEGINNING, "name": "Kitchen"},
        ),
    )

    assert [(r["area_id"], r["area_name"]) for r in rows] == [("kitchen", "Kitchen")]


def test_device_name_given_by_the_user_wins_over_the_integrations_name():
    rows = enrich(
        events(("sensor.fan_power", at(9)), ("sensor.lamp_power", at(9))),
        entities(
            {"entity_id": "sensor.fan_power", "valid_from": BEGINNING, "device_id": "dev1"},
            {"entity_id": "sensor.lamp_power", "valid_from": BEGINNING, "device_id": "dev2"},
        ),
        devices(
            {"device_id": "dev1", "valid_from": BEGINNING, "name": "Smart Plug", "name_by_user": "Fan plug"},
            {"device_id": "dev2", "valid_from": BEGINNING, "name": "Smart Plug", "name_by_user": None},
        ),
        areas(),
    )

    assert [r["device_name"] for r in rows] == ["Fan plug", "Smart Plug"]


def test_event_with_no_version_in_force_is_kept_without_names():
    rows = enrich(
        events(("sensor.unknown", at(9))),
        entities(),
        devices(),
        areas(),
    )

    assert [(r["entity_id"], r["entity_name"], r["device_name"], r["area_name"]) for r in rows] == [
        ("sensor.unknown", None, None, None)
    ]
