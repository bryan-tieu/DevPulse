from datetime import date, datetime
from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from google.api_core import exceptions as gcp_exceptions
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from pydantic import BaseModel, ValidationError

from api.cache import QueryCache, cache_run_query, get_query_cache
from api.queries import (
    build_language_momentum_query,
    build_leaderboard_query,
    build_trending_query,
)
from config import BQ_DATASET, GCP_PROJECT

app = FastAPI(
    title="DevPulse",
    description="Developer-ecosystem analytics over the GitHub event stream.",
)


@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:

    return bigquery.Client(project=GCP_PROJECT)


class TrendingRepo(BaseModel):
    date_key: int
    repo_id: int
    repo_name: str
    stars: int
    daily_rank: int


class TrendingResponse(BaseModel):
    date: date
    limit: int
    offset: int
    results: list[TrendingRepo]


class LanguageMomentumRepo(BaseModel):
    date_key: int
    language: str
    event_count: int
    active_repos: int
    # For testing purposes, momentum delta takes previous day values
    # so if there is only one row in storage, delta becomes NULL
    momentum_delta: int | None
    daily_rank: int


class LanguageMomentumResponse(BaseModel):
    date: date
    limit: int
    offset: int
    results: list[LanguageMomentumRepo]


class LeaderboardActor(BaseModel):
    date_key: int
    actor_id: int
    actor_login: str
    contributions: int
    daily_rank: int


class LeaderboardResponse(BaseModel):
    date: date
    limit: int
    offset: int
    results: list[LeaderboardActor]


class TaskState(BaseModel):
    task_id: str
    state: str
    duration: float | None


class PipelineRun(BaseModel):
    run_id: str
    logical_date: datetime
    tasks: list[TaskState]
    raw_rows: int | None
    hour_rows: int | None
    quarantine_rows: int | None
    residual_rows: int | None
    recorded_at: datetime
    verdict: bool | None


class RunError(BaseModel):
    run_id: str
    reason: str


class RunsResponse(BaseModel):
    limit: int
    results: list[PipelineRun]
    errors: list[RunError]


@app.get("/trending", response_model=TrendingResponse)
def trending(
    response: Response,
    client: bigquery.Client = Depends(get_bq_client),
    cache: QueryCache = Depends(get_query_cache),
    day: date = Query(alias="date", description="Partition date to read"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=10_000),
) -> TrendingResponse:

    date_key = int(day.strftime("%Y%m%d"))

    sql, params = build_trending_query(date_key, limit, offset)

    try:
        rows, hit = cache_run_query(client, sql, params, cache)

    except gcp_exceptions.GoogleAPIError as e:

        # exception class is the library fallback; use
        # job's reason to verify what gets called
        capped = any(err.get("reason") == "bytesBilledLimitExceeded" for err in e.errors)
        if capped:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Query exceeded configured byte budget",
            ) from e

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Upstream warehouse error"
        ) from e

    response.headers["X-Cache"] = "hit" if hit else "miss"

    return TrendingResponse(
        date=day, limit=limit, offset=offset, results=[TrendingRepo(**row) for row in rows]
    )


@app.get("/languages/momentum", response_model=LanguageMomentumResponse)
def language_momentum(
    response: Response,
    client: bigquery.Client = Depends(get_bq_client),
    cache: QueryCache = Depends(get_query_cache),
    day: date = Query(alias="date", description="Partition date to read"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100_000),
) -> LanguageMomentumResponse:

    date_key = int(day.strftime("%Y%m%d"))

    sql, params = build_language_momentum_query(date_key, limit, offset)

    try:
        rows, hit = cache_run_query(client, sql, params, cache)

    except gcp_exceptions.GoogleAPIError as e:
        capped = any(err.get("reason") == "bytesBilledLimitExceeded" for err in e.errors)

        if capped:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Query exceeded configured byte budget",
            ) from e

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Upstream warehouse error"
        ) from e

    response.headers["X-Cache"] = "hit" if hit else "miss"

    return LanguageMomentumResponse(
        date=day, limit=limit, offset=offset, results=[LanguageMomentumRepo(**row) for row in rows]
    )


@app.get("/leaderboard", response_model=LeaderboardResponse)
def leaderboard(
    response: Response,
    client: bigquery.Client = Depends(get_bq_client),
    cache: QueryCache = Depends(get_query_cache),
    day: date = Query(alias="date", description="Partition date to read"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=50_000),
) -> LeaderboardResponse:

    date_key = int(day.strftime("%Y%m%d"))
    sql, params = build_leaderboard_query(date_key, limit, offset)

    try:
        rows, hit = cache_run_query(client, sql, params, cache)

    except gcp_exceptions.GoogleAPIError as e:
        capped = any(err.get("reason") == "bytesBilledLimitExceeded" for err in e.errors)

        if capped:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Query exceeded configured byte budget",
            ) from e

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Upstream warehouse error"
        ) from e

    response.headers["X-Cache"] = "hit" if hit else "miss"

    return LeaderboardResponse(
        date=day, limit=limit, offset=offset, results=[LeaderboardActor(**row) for row in rows]
    )


@app.get("/runs", response_model=RunsResponse)
def runs(
    client: bigquery.Client = Depends(get_bq_client),
    limit: int = Query(10, ge=1, le=100),
) -> RunsResponse:

    TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.pipeline_run_metadata"

    try:

        # at scale, the table grows from just a single cheap read;
        # partition to ensure we get the rows we actually want at scale
        rows = list(client.list_rows(TABLE_ID))
    except NotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Table {TABLE_ID} not found."
        )
    except Forbidden:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Missing BigQuery permissions"
        )
    except gcp_exceptions.GoogleAPIError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Upstream warehouse error"
        ) from e

    runs, errors = [], []
    for row in rows:
        try:
            tasks = row["tasks"] if row["tasks"] else []
            summary = row["run_summary_json"] if row["run_summary_json"] else {}

            run = PipelineRun(
                run_id=row["run_id"],
                logical_date=row["logical_date"],
                recorded_at=row["recorded_at"],
                tasks=tasks,
                raw_rows=row["raw_rows"],
                hour_rows=row["hour_rows"],
                quarantine_rows=row["quarantine_rows"],
                residual_rows=row["residual_rows"],
                verdict=summary.get("pipeline_check"),
            )

            runs.append(run)

        except ValidationError as e:

            errors.append(RunError(run_id=row["run_id"], reason=repr(e)))
    runs.sort(key=lambda r: r.recorded_at, reverse=True)
    runs = runs[:limit]

    return RunsResponse(limit=limit, results=runs, errors=errors)
