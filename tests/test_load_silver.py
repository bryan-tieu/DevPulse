"""Unit tests for the pure partition-derivation in transform/load_silver.

Plain Python (no GCS, no BigQuery) — runs on a host checkout / .venv, unlike the
Spark tests which need PySpark + Java in the container.
"""

from transform.load_silver import _hour_partition


def test_hour_partition_two_digit_hour():
    path, decorator = _hour_partition("2024-01-01", 15)
    assert path == "events/event_date=2024-01-01/event_hour=15/*.parquet"
    assert decorator == "2024010115"


def test_hour_partition_single_digit_hour_padding_asymmetry():
    # The real gotcha: the silver PATH is UNPADDED (event_hour=5, from Spark's
    # int hour()), but the BQ decorator zero-pads (...0105). Confusing the two
    # yields an empty glob or a wrong-partition load.
    path, decorator = _hour_partition("2024-01-01", 5)
    assert path == "events/event_date=2024-01-01/event_hour=5/*.parquet"
    assert decorator == "2024010105"
