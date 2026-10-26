from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from dagster_home_automation.home_assistant.versions import build_versions

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
CREATED = datetime(2026, 9, 1, tzinfo=timezone.utc)

TRACKED = ["name", "area_id"]
SCHEMA = {
    "entity_id": pl.Utf8,
    "snapshot_time": pl.Datetime("us", "UTC"),
    "created_at": pl.Datetime("us", "UTC"),
    "modified_at": pl.Datetime("us", "UTC"),
    "name": pl.Utf8,
    "area_id": pl.Utf8,
}


def hour(n: float) -> datetime:
    return T0 + timedelta(hours=n)


def member(entity_id: str, at: float, *, created_at=CREATED, modified_at=CREATED, **attributes) -> dict:
    row = {"entity_id": entity_id, "snapshot_time": hour(at), "created_at": created_at, "modified_at": modified_at}
    row.update({"name": None, "area_id": None, **attributes})
    return row


def versions_of(*rows: dict) -> list[dict]:
    snapshots = pl.DataFrame(list(rows), schema=SCHEMA)
    return build_versions(snapshots, key="entity_id", tracked=TRACKED).sort("entity_id", "valid_from").to_dicts()


def test_member_that_never_changes_has_one_current_version_starting_at_creation():
    versions = versions_of(
        member("light.fan", 0, name="Fan", area_id="office"),
        member("light.fan", 1, name="Fan", area_id="office"),
        member("light.fan", 2, name="Fan", area_id="office"),
    )

    assert versions == [
        {
            "entity_id": "light.fan",
            "valid_from": CREATED,
            "valid_to": None,
            "last_seen": hour(2),
            "is_removed": False,
            "name": "Fan",
            "area_id": "office",
        }
    ]


def test_tracked_change_starts_a_new_version_at_modified_at_when_it_falls_in_the_snapshot_gap():
    renamed_at = hour(1.5)
    versions = versions_of(
        member("light.fan", 0, name="Fan"),
        member("light.fan", 1, name="Fan"),
        member("light.fan", 2, name="Desk fan", modified_at=renamed_at),
        member("light.fan", 3, name="Desk fan", modified_at=renamed_at),
    )

    assert [(v["name"], v["valid_from"], v["valid_to"], v["last_seen"]) for v in versions] == [
        ("Fan", CREATED, renamed_at, hour(1)),
        ("Desk fan", renamed_at, None, hour(3)),
    ]


@pytest.mark.parametrize(
    "modified_at",
    [
        pytest.param(CREATED, id="not bumped by the change"),
        pytest.param(hour(1), id="at the last snapshot still showing the old values"),
        pytest.param(hour(2.5), id="after the first snapshot showing the change"),
        pytest.param(None, id="missing"),
    ],
)
def test_change_falls_back_to_first_snapshot_showing_it_when_modified_at_is_outside_the_gap(modified_at):
    versions = versions_of(
        member("light.fan", 1, name="Fan"),
        member("light.fan", 2, name="Desk fan", modified_at=modified_at),
    )

    assert [(v["name"], v["valid_from"], v["valid_to"]) for v in versions] == [
        ("Fan", CREATED, hour(2)),
        ("Desk fan", hour(2), None),
    ]


@pytest.mark.parametrize(
    "created_at",
    [
        pytest.param(None, id="missing"),
        pytest.param(hour(1.5), id="after the first snapshot"),
    ],
)
def test_first_version_starts_at_first_snapshot_when_created_at_is_unusable(created_at):
    versions = versions_of(
        member("light.fan", 1, name="Fan", created_at=created_at),
        member("light.fan", 2, name="Fan", created_at=created_at),
    )

    assert [v["valid_from"] for v in versions] == [hour(1)]


def test_attribute_that_stays_null_does_not_start_new_versions():
    versions = versions_of(
        member("light.fan", 0, name="Fan", area_id=None),
        member("light.fan", 1, name="Fan", area_id=None),
        member("light.fan", 2, name="Fan", area_id=None),
    )

    assert len(versions) == 1


def test_attribute_gaining_or_losing_a_value_starts_a_new_version():
    versions = versions_of(
        member("light.fan", 0, area_id=None),
        member("light.fan", 1, area_id="office"),
        member("light.fan", 2, area_id=None),
    )

    assert [v["area_id"] for v in versions] == [None, "office", None]


def test_member_missing_from_a_snapshot_is_removed_keeping_its_last_known_attributes():
    versions = versions_of(
        member("light.fan", 0, name="Fan"),
        member("light.lamp", 0, name="Lamp"),
        member("light.fan", 1, name="Fan"),
        member("light.lamp", 1, name="Lamp"),
        member("light.lamp", 2, name="Lamp"),
        member("light.lamp", 3, name="Lamp"),
    )

    fan = [v for v in versions if v["entity_id"] == "light.fan"]
    assert [(v["name"], v["valid_from"], v["valid_to"], v["last_seen"], v["is_removed"]) for v in fan] == [
        ("Fan", CREATED, hour(2), hour(1), False),
        ("Fan", hour(2), None, hour(3), True),
    ]


def test_removed_member_that_comes_back_starts_a_new_version():
    readded_at = hour(1.5)
    versions = versions_of(
        member("light.fan", 0, name="Fan"),
        member("light.lamp", 0, name="Lamp"),
        member("light.lamp", 1, name="Lamp"),
        member("light.fan", 2, name="Fan", modified_at=readded_at),
        member("light.lamp", 2, name="Lamp"),
    )

    fan = [v for v in versions if v["entity_id"] == "light.fan"]
    assert [(v["valid_from"], v["valid_to"], v["is_removed"]) for v in fan] == [
        (CREATED, hour(1), False),
        (hour(1), readded_at, True),
        (readded_at, None, False),
    ]


def test_removed_version_keeps_attributes_that_were_null_when_last_seen():
    versions = versions_of(
        member("light.fan", 0, name="Fan", area_id="office"),
        member("light.lamp", 0),
        member("light.fan", 1, name="Fan", area_id=None),
        member("light.lamp", 1),
        member("light.lamp", 2),
    )

    fan = [v for v in versions if v["entity_id"] == "light.fan"]
    assert [(v["area_id"], v["is_removed"]) for v in fan] == [("office", False), (None, False), (None, True)]


def test_hour_with_no_snapshot_at_all_does_not_remove_anyone():
    versions = versions_of(
        member("light.fan", 0, name="Fan"),
        member("light.fan", 3, name="Fan"),
    )

    assert [(v["valid_to"], v["last_seen"], v["is_removed"]) for v in versions] == [(None, hour(3), False)]


def test_change_only_to_untracked_attributes_does_not_start_a_new_version():
    versions = versions_of(
        member("light.fan", 0, name="Fan"),
        member("light.fan", 1, name="Fan", modified_at=hour(0.5)),
    )

    assert len(versions) == 1
