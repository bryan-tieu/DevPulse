"""Unit tests for the pure silver transform.

These exercise `transform_events` on a tiny in-memory DataFrame — no GCS, no
bronze read, no write. That's only possible because the transform is a pure
DataFrame->DataFrame function with the I/O kept out in `run()`.
"""

import datetime

import pytest
from pyspark.sql import SparkSession
from silver_events import SCHEMA, transform_events


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("silver-tests").getOrCreate()
    yield session
    session.stop()


def _raw_rows():
    # Shape matches SCHEMA: (id, type, public, created_at, actor(id,login), repo(id,name))
    return [
        ("1", "PushEvent", True, "2024-01-01T15:00:00Z", (1, "alice"), (10, "a/b")),
        ("1", "PushEvent", True, "2024-01-01T15:00:00Z", (1, "alice"), (10, "a/b")),  # dup id
        ("2", "WatchEvent", True, "2024-01-01T15:30:00Z", (2, "bob"), (20, "c/d")),
        ("3", "ForkEvent", True, "not-a-timestamp", (3, "carol"), (30, "e/f")),  # bad ts
    ]


def test_dedupe_collapses_duplicate_event_ids(spark):
    raw = spark.createDataFrame(_raw_rows(), schema=SCHEMA)

    out = transform_events(raw)

    # 4 raw rows, two of which share event_id "1" -> 3 distinct events remain.
    assert out.count() == 3
    assert out.select("event_id").distinct().count() == 3


def test_flatten_cast_and_partition_columns(spark):
    raw = spark.createDataFrame(_raw_rows(), schema=SCHEMA)

    rows = {r["event_id"]: r for r in transform_events(raw).collect()}

    good = rows["2"]
    assert good["actor_login"] == "bob"  # nested actor.login flattened
    assert good["repo_name"] == "c/d"  # nested repo.name flattened
    assert good["event_date"] == datetime.date(2024, 1, 1)  # derived from created_at
    assert good["event_hour"] == 15

    # A malformed created_at casts to NULL rather than crashing the job, and the
    # derived partition columns follow it to NULL.
    bad = rows["3"]
    assert bad["created_at"] is None
    assert bad["event_date"] is None
    assert bad["event_hour"] is None
