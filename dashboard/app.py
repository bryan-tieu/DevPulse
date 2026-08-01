import datetime
from datetime import date

import streamlit as st
from api_client import DashboardError, get_language_momentum, get_leaderboard, get_trending

st.set_page_config(
    page_title="DevPulse",
    initial_sidebar_state="expanded",
)

st.title("Welcome to DevPulse")

st.sidebar.title("Date Selection")


user_date_input = st.sidebar.date_input("Select a date: ", value=datetime.date(2024, 1, 1))

user_limit_input = st.sidebar.slider("Limit: ", min_value=1, max_value=100, value=1)


def _build_panel(
    fetch,
    day: date,
    limit: int,
    x: str,
    y: str,
    label: str,
    display_null_cols: tuple[str, ...] = (),
    unknown_bucket: tuple[str, str] | None = None,
) -> None:

    try:
        rows = fetch(day=day, limit=limit)["results"]

        if not rows:
            st.info(f"No {label} data for {day}")
            return

        # The table keeps every row, the chart narrows to known values,
        # and the caption is page-scoped because the API gives no day-level
        # totals
        if unknown_bucket:
            col, sentinel = unknown_bucket
            chart_rows = [r for r in rows if r[col] != sentinel]
            total = sum(r[y] for r in rows)
            unknown = total - sum(r[y] for r in chart_rows)
            share = unknown / total if total else 0
        else:
            chart_rows = rows

        # `—` goes to the table only — the chart needs the numeric rows, and a
        # string in a charted column breaks it. NULL here means the day-over-day
        # window is degenerate (one ingested hour), not that the delta is zero.
        table_rows = [
            {**r, **{c: ("—" if r.get(c) is None else r[c]) for c in display_null_cols}}
            for r in rows
        ]
        st.dataframe(table_rows)

        if not chart_rows and unknown_bucket:
            st.info(f"No known language data available for {day}")
        else:
            st.bar_chart(chart_rows, x=x, y=y)

        if unknown_bucket and chart_rows:
            st.caption(
                f"Of the languages shown, Unknown is {share:.1%} "
                f"of events ({unknown:,} of {total:,}); "
                f"bars show the {1 - share:.1%} with a resolved language"
            )

    except DashboardError as e:
        st.error(str(e))


PANELS = [
    dict(fetch=get_trending, x="repo_name", y="stars", label="trending"),
    dict(fetch=get_leaderboard, x="actor_login", y="contributions", label="leaderboard"),
    dict(
        fetch=get_language_momentum,
        x="language",
        y="event_count",
        label="language momentum",
        display_null_cols=("momentum_delta",),
        unknown_bucket=("language", "Unknown"),
    ),
]

for panel in PANELS:
    _build_panel(day=user_date_input, limit=user_limit_input, **panel)
