"""Host-runnable tests for the pure half of transform/load_pipeline_metadata.

Covers the row builder (schema-drift guard, NULL paths, JSON-safety) and the
summary staleness check (the happens-before invariant + partition match).
BQ I/O (_ensure_table, the load job) is deliberately untested — that's the
other side of the pure/I-O seam; pytest there would be testing Google.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from transform.load_pipeline_metadata import SCHEMA, build_metadata_row, summary_is_fresh

RUN_START = datetime(2026, 7, 11, 4, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE = datetime(2024, 1, 1, 15, 0, 0, tzinfo=timezone.utc)


def make_task(task_id: str = "ingest", state: str = "success", duration: float | None = 12.5):
    """Fake TaskInstance: attribute access, mirroring the real object's API."""
    return SimpleNamespace(task_id=task_id, state=state, duration=duration)


def make_summary(**overrides) -> dict:
    """A canonical-hour summary as json.load would return it: timestamps are
    STRINGS, written after RUN_START (i.e. fresh) unless a test overrides."""
    summary = {
        "raw": 180387,
        "hour": 180386,
        "quarantine_rows": 0,
        "residual_rows": 1,
        "quarantine_check": True,
        "residual_check": True,
        "great_expectations_suite_check": True,
        "pipeline_check": True,
        "partition_date": "2024-01-01",
        "partition_hour": 15,
        "timestamp": "2026-07-11T04:05:00+00:00",
    }
    summary.update(overrides)
    return summary


@pytest.fixture
def row_kwargs():
    """Factory fixture: full happy-path inputs; tests override one field at a time."""

    def _make(**overrides) -> dict:
        kwargs = {
            "run_id": "backfill__2024-01-01T15:00:00+00:00",
            "recorded_at": "2026-07-11T04:10:00+00:00",
            "logical_date": LOGICAL_DATE.isoformat(),
            "run_summary": make_summary(),
            "raw_rows": 180387,
            "hour_rows": 180386,
            "quarantine_rows": 0,
            "residual_rows": 1,
            "task_instances": [make_task(), make_task("dbt_build", "failed", None)],
        }
        kwargs.update(overrides)
        return kwargs

    return _make


_NULLED = {
    "run_summary": None,
    "raw_rows": None,
    "hour_rows": None,
    "quarantine_rows": None,
    "residual_rows": None,
}


def _count_fields() -> list[str]:
    """The four count columns, by whatever names SCHEMA declares (schema order)."""
    return [f.name for f in SCHEMA if f.field_type == "INTEGER"]


# ── the row builder ──────────────────────────────────────────────────────────


def test_row_keys_match_schema(row_kwargs):
    # THE drift guard: builder output and SCHEMA must name the same fields,
    # in both directions. Any rename that touches only one side fails here.
    row = build_metadata_row(**row_kwargs())
    assert set(row) == {f.name for f in SCHEMA}


def test_happy_path_values(row_kwargs):
    row = build_metadata_row(**row_kwargs())
    assert row["run_id"] == "backfill__2024-01-01T15:00:00+00:00"
    assert row["logical_date"] == "2024-01-01T15:00:00+00:00"
    # counts land in the four INTEGER columns in schema order
    assert [row[name] for name in _count_fields()] == [180387, 180386, 0, 1]
    # task instances are flattened to plain dicts, None duration preserved
    assert row["tasks"][1] == {"task_id": "dbt_build", "state": "failed", "duration": None}


def test_missing_summary_records_nulls(row_kwargs):
    # A run that died before validate_silver still gets a row: counts NULL,
    # identity fields populated. This is the path the observer exists for.
    row = build_metadata_row(**row_kwargs(**_NULLED))
    for name in _count_fields():
        assert row[name] is None
    assert row["run_summary_json"] is None
    assert row["run_id"]
    assert row["recorded_at"]
    assert row["tasks"]


def test_row_is_json_serializable(row_kwargs):
    # load_table_from_json needs plain JSON types on every path — a raw
    # datetime sneaking into the row fails here, not at 2am in the load job.
    json.dumps(build_metadata_row(**row_kwargs()))
    json.dumps(build_metadata_row(**row_kwargs(**_NULLED)))


# ── the staleness check (happens-before invariant) ───────────────────────────


def test_fresh_summary_accepted():
    assert summary_is_fresh(make_summary(), RUN_START, LOGICAL_DATE) is True


def test_summary_written_before_run_start_is_stale():
    # Written 1s before the run started -> cannot be this run's output.
    old = make_summary(timestamp="2026-07-11T03:59:59+00:00")
    assert summary_is_fresh(old, RUN_START, LOGICAL_DATE) is False


def test_summary_for_wrong_partition_is_stale():
    # The concurrent-overwrite case: timestamp looks fresh, but the summary
    # belongs to a different hour/date -> partition match must reject it.
    assert summary_is_fresh(make_summary(partition_hour=17), RUN_START, LOGICAL_DATE) is False
    assert (
        summary_is_fresh(make_summary(partition_date="2024-01-02"), RUN_START, LOGICAL_DATE)
        is False
    )
