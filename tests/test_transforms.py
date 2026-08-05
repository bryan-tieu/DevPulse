import pytest

from dashboard.transforms import SchemaError, mask_nulls, split_unknown


def test_split_unknown_excludes_sentinel_keeps_orders():
    rows = [
        {"language": "Python", "repo_count": 10},
        {"language": "Unknown", "repo_count": 7},
        {"language": "JavaScript", "repo_count": 3},
    ]

    split = split_unknown(rows, "language", "Unknown", "repo_count")

    assert split.chart_rows == [
        {"language": "Python", "repo_count": 10},
        {"language": "JavaScript", "repo_count": 3},
    ]


def test_split_unknown_share_uses_total_as_denominator():
    rows = [
        {"language": "Unknown", "repo_count": 178_081},
        {"language": "Python", "repo_count": 1_500},
        {"language": "JavaScript", "repo_count": 600},
        {"language": "Rust", "repo_count": 205},
    ]

    split = split_unknown(rows, "language", "Unknown", "repo_count")

    assert split.unknown == 178_081
    assert split.total == 180_386
    assert split.share == pytest.approx(0.9872, abs=1e-4)


def test_split_unknown_with_no_sentinel_row():
    rows = [
        {"language": "Python", "repo_count": 10},
        {"language": "JavaScript", "repo_count": 3},
    ]

    split = split_unknown(rows, "language", "Unknown", "repo_count")

    assert split.chart_rows == rows
    assert split.unknown == 0
    assert split.total == 13
    assert split.share == 0.0


def test_split_unknown_every_row_is_sentinel():

    rows = [
        {"language": "Unknown", "repo_count": 178_081},
        {"language": "Unknown", "repo_count": 19},
    ]

    split = split_unknown(rows, "language", "Unknown", "repo_count")

    assert split.chart_rows == []
    assert split.unknown == split.total == 178_100
    assert split.share == 1.0


def test_split_unknown_with_empty_rows():
    split = split_unknown([], "language", "Unknown", "repo_count")

    assert split.chart_rows == []
    assert split.unknown == split.total == 0
    assert split.share == 0.0


def test_split_unknown_raises_measure_key_is_missing():

    rows = [{"language": "Python"}]

    with pytest.raises(SchemaError):
        split_unknown(rows, "language", "Unknown", "repo_count")


def test_mask_nulls_replaces_none_with_placeholder():
    rows = [{"language": "Python", "momentum_delta": None}]

    masked = mask_nulls(rows, ("momentum_delta",))

    assert masked == [{"language": "Python", "momentum_delta": "—"}]


@pytest.mark.parametrize("value", [0, 0.0, -5, False])
def test_mask_nulls_preserves_falsy_measured_values(value):

    rows = [{"language": "Python", "momentum_delta": value}]

    masked = mask_nulls(rows, ("momentum_delta",))

    assert masked[0]["momentum_delta"] is value


def test_mask_nulls_leaves_columns_outside_cols_untouched():
    rows = [{"language": None, "stars": None, "momentum_delta": None}]

    masked = mask_nulls(rows, ("momentum_delta",))

    assert masked == [{"language": None, "stars": None, "momentum_delta": "—"}]


def test_mask_nulls_does_not_mutate_input_rows():

    rows = [{"language": "Python", "momentum_delta": None}]

    masked = mask_nulls(rows, ("momentum_delta",))

    assert rows == [{"language": "Python", "momentum_delta": None}]

    assert masked[0] is not rows[0]


def test_mask_nulls_with_no_columns():

    rows = [{"language": "Python", "momentum_delta": None}]

    masked = mask_nulls(rows, ())

    assert masked == [{"language": "Python", "momentum_delta": None}]


def test_mask_nulls_honors_placeholder():

    rows = [{"language": "Python", "momentum_delta": None}]

    masked = mask_nulls(rows, ("momentum_delta",), placeholder="N/A")

    assert masked == [{"language": "Python", "momentum_delta": "N/A"}]


def test_mask_nulls_missing_key():
    rows = [{"language": "Python", "momentum_change": None}]

    with pytest.raises(SchemaError, match="momentum_delta"):
        mask_nulls(rows, ("momentum_delta",))
