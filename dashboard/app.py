import datetime
from datetime import date
from typing import Any, Protocol

import streamlit as st

from dashboard.api_client import (
    DashboardError,
    get_language_momentum,
    get_leaderboard,
    get_runs,
    get_trending,
)
from dashboard.transforms import SchemaError, mask_nulls, split_unknown


class RankedFetch(Protocol):
    def __call__(self, *, day: date, limit: int) -> dict[str, Any]: ...


st.set_page_config(
    page_title="DevPulse",
    initial_sidebar_state="expanded",
)

st.title("Welcome to DevPulse")

st.sidebar.title("Date Selection")

st.sidebar.button("Clear Cache Data", on_click=st.cache_data.clear)

user_date_input = st.sidebar.date_input("Select a date: ", value=datetime.date(2024, 1, 1))

user_limit_input = st.sidebar.slider("Limit: ", min_value=1, max_value=100, value=1)

RANKED_TTL = 5 * 60

# Remains uncached server side but given 30 second cache
# run metadata gets updated every hour at the minimum
# A 30 second cache sits well below the update rate
RUNS_TTL = 30


@st.cache_data(ttl=RANKED_TTL)
def fetch_trending(day: date, limit: int) -> dict:
    return get_trending(day=day, limit=limit)


@st.cache_data(ttl=RANKED_TTL)
def fetch_leaderboard(day: date, limit: int) -> dict:
    return get_leaderboard(day=day, limit=limit)


@st.cache_data(ttl=RANKED_TTL)
def fetch_language_momentum(day: date, limit: int) -> dict:
    return get_language_momentum(day=day, limit=limit)


@st.cache_data(ttl=RUNS_TTL)
def fetch_runs(limit: int) -> dict:
    return get_runs(limit)


def _build_ranked_panel(
    fetch: RankedFetch,
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
            st.header(f"{label.capitalize()} chart for {day}")
            st.bar_chart(chart_rows, x=x, y=y)

        if unknown_bucket and unknown > 0:
            st.caption(
                f"Of the languages shown, Unknown is {share:.1%} "
                f"of events ({unknown:,} of {total:,}); "
                f"bars show the {1 - share:.1%} with a resolved language"
            )

    except DashboardError as e:
        st.error(str(e))
    # A separate clause on purpose: a transport failure is an outage the
    # operator waits out, a schema mismatch is a bug someone has to fix.
    except SchemaError as e:
        st.error(f"Dashboard bug — {e}")


def _build_runs_panel(limit: int = 10) -> None:

    st.header("Pipeline Status")

    try:
        resp = fetch_runs(limit)

        result = resp["results"]
        errors = resp["errors"]

        for error in errors:
            st.warning(f"Malformed row: Run ID: {error['run_id']}. {error['reason']}")

        if not result:

            # Empty because of unparseable runs, Not because of failures
            st.info("No readable runs could be parsed")
            return

        newest_run_block = result[0]

        if newest_run_block["verdict"] is True:
            st.header("Passed")
        elif newest_run_block["verdict"] is False:
            st.header("Failed")
        else:
            st.header("Unknown")

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
    dict(fetch=fetch_trending, x="repo_name", y="stars", label="trending"),
    dict(fetch=fetch_leaderboard, x="actor_login", y="contributions", label="leaderboard"),
    dict(
        fetch=fetch_language_momentum,
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
