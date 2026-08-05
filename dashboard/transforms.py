from typing import NamedTuple


class SchemaError(Exception):
    """A payload is missing a column this module was told to expect.

    Deliberately *not* a `DashboardError`: that one means the call to the API
    failed. This means the call succeeded and the body is the wrong shape —
    a renamed field, version skew against the API, or a typo in a panel's
    column config. All three are bugs, never a data state, so they must not
    render like one.
    """


class UnknownSplit(NamedTuple):
    chart_rows: list[dict]
    unknown: int
    total: int
    share: float


def _require_columns(rows: list[dict], cols: tuple[str, ...]) -> None:
    missing = sorted({c for row in rows for c in cols if c not in row})
    if missing:
        raise SchemaError(
            f"API response is missing column(s): {', '.join(missing)}. "
            "The response contract changed, or a panel names a column that does not exist."
        )


def split_unknown(rows: list[dict], col: str, sentinel: str, measure: str) -> UnknownSplit:
    _require_columns(rows, (col, measure))

    chart_rows = [r for r in rows if r[col] != sentinel]
    total = sum(r[measure] for r in rows)
    unknown = total - sum(r[measure] for r in chart_rows)
    share = unknown / total if total else 0.0

    return UnknownSplit(chart_rows, unknown, total, share)


# `—` goes to the table only — the chart needs the numeric rows, and a
# string in a charted column breaks it. NULL here means the day-over-day
# window is degenerate (one ingested hour), not that the delta is zero.
#
# An absent key is a *different* failure and raises above. `.get()` with a
# default would have rendered schema drift as `—`, making a broken contract
# indistinguishable from an empty column — the same information loss as an
# empty chart reading "zero activity", one layer up.
def mask_nulls(rows: list[dict], cols: tuple[str, ...], placeholder: str = "—") -> list[dict]:
    _require_columns(rows, cols)

    return [{**row, **{c: placeholder if row[c] is None else row[c] for c in cols}} for row in rows]
