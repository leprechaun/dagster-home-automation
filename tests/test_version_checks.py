from datetime import datetime, timezone

import polars as pl
import pytest

from dagster_home_automation.defs.assets.silver_home_assistant import area_versions_are_well_formed


def at(hour: int) -> datetime:
    return datetime(2026, 10, 1, hour, tzinfo=timezone.utc)


def area_versions(*spans: tuple[str, datetime, datetime | None]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"area_id": area_id, "valid_from": start, "valid_to": end} for area_id, start, end in spans],
        schema={"area_id": pl.Utf8, "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    )


def test_back_to_back_versions_with_one_current_pass():
    result = area_versions_are_well_formed(
        area_versions(("office", at(0), at(2)), ("office", at(2), None), ("kitchen", at(0), None))
    )

    assert result.passed


@pytest.mark.parametrize(
    "versions, broken_rule",
    [
        pytest.param(
            area_versions(("office", at(0), at(3)), ("office", at(2), None)),
            "overlapping",
            id="overlapping versions",
        ),
        pytest.param(
            area_versions(("office", at(0), None), ("office", at(2), None)),
            "not_exactly_one_current",
            id="two current versions",
        ),
        pytest.param(
            area_versions(("office", at(0), at(2))),
            "not_exactly_one_current",
            id="no current version",
        ),
        pytest.param(
            area_versions(("office", at(2), at(2)), ("office", at(2), None)),
            "empty_or_backwards",
            id="version ending when it starts",
        ),
    ],
)
def test_broken_versions_fail_naming_the_rule_and_the_keys(versions, broken_rule):
    result = area_versions_are_well_formed(versions)

    assert not result.passed
    assert result.metadata[broken_rule].value == ["office"]
