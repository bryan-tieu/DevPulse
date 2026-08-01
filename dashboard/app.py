import datetime
from datetime import date

import streamlit as st

from dashboard.api_client import (
    DashboardError,
    get_language_momentum,
    get_leaderboard,
    get_runs,
    get_trending,
)
from dashboard.transforms import mask_nulls, split_unknown

st.set_page_config(
    page_title="DevPulse",
    initial_sidebar_state="expanded",
)

st.title("Welcome to DevPulse")

st.sidebar.title("Date Selection")


user_date_input = st.sidebar.date_input("Select a date: ", value=datetime.date(2024, 1, 1))

user_limit_input = st.sidebar.slider("Limit: ", min_value=1, max_value=100, value=1)


def _build_ranked_panel(
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
            split = split_unknown(rows, col, sentinel, y)

            chart_rows = split.chart_rows
            total = split.total
            unknown = split.unknown
            share = split.share
        else:
            chart_rows = rows

        table_rows = mask_nulls(rows, display_null_cols)
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


def _build_runs_panel(limit: int = 10) -> None:

    st.header("Pipeline Status")

    try:
        resp = get_runs(limit)

        result = resp["results"]
        errors = resp["errors"]

        if not result:
            st.info("No runs available")
            return

        if errors:
            for error in errors:
                st.warning(f"Malformed row: Run ID: {error['run_id']}. {error['reason']}")

        newest_run_block = result[0]

        if newest_run_block["verdict"] is True:
            st.caption("Passed")
        elif newest_run_block["verdict"] is False:
            st.caption("Failed")
        else:
            st.caption("Unknown ")

        st.caption(newest_run_block["run_id"])
        st.caption(f"Logical date: {newest_run_block['logical_date']}")
        st.caption(f"Recorded at: {newest_run_block['recorded_at']}")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(label="raw", value=newest_run_block["raw_rows"])
        col2.metric(label="hour", value=newest_run_block["hour_rows"])
        col3.metric(label="quarantine", value=newest_run_block["quarantine_rows"])
        col4.metric(label="residual", value=newest_run_block["residual_rows"])

        st.subheader("Tasks")
        st.dataframe(newest_run_block["tasks"])

        st.header("Run History")
        st.dataframe(result)

    except DashboardError as e:
        if e.status_code == 404:
            st.info("No pipeline runs recorded yet")
        else:
            st.error(str(e))


RANKED_PANELS = [
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

for panel in RANKED_PANELS:
    _build_ranked_panel(day=user_date_input, limit=user_limit_input, **panel)

_build_runs_panel()
