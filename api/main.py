from datetime import date
from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery
from pydantic import BaseModel

from api.cache import QueryCache, cache_run_query, get_query_cache
from api.queries import (
    build_language_momentum_query,
    build_leaderboard_query,
    build_trending_query,
)
from config import GCP_PROJECT

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
