from typing import NamedTuple


class UnknownSplit(NamedTuple):
    chart_rows: list[dict]
    unknown: int
    total: int
    share: float


def split_unknown(rows: list[dict], col: str, sentinel: str, measure: str) -> UnknownSplit:
    chart_rows = [r for r in rows if r[col] != sentinel]
    total = sum(r[measure] for r in rows)
    unknown = total - sum(r[measure] for r in chart_rows)
    share = unknown / total if total else 0.0

    return UnknownSplit(chart_rows, unknown, total, share)


# `—` goes to the table only — the chart needs the numeric rows, and a
# string in a charted column breaks it. NULL here means the day-over-day
# window is degenerate (one ingested hour), not that the delta is zero.
def mask_nulls(rows: list[dict], cols: tuple[str, ...], placeholder: str = "—") -> list[dict]:
    return [
        {**row, **{c: placeholder if row.get(c) is None else row[c] for c in cols}} for row in rows
    ]
