from datetime import date
from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query, status
from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery
from pydantic import BaseModel

from api.queries import build_trending_query, run_query
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


@app.get("/trending", response_model=TrendingResponse)
def trending(
    client: bigquery.Client = Depends(get_bq_client),
    day: date = Query(alias="date", description="Partition date to read"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=10_000),
) -> TrendingResponse:

    date_key = int(day.strftime("%Y%m%d"))

    sql, params = build_trending_query(date_key, limit, offset)

    try:
        rows = run_query(client, sql, params)

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

    return TrendingResponse(
        date=day, limit=limit, offset=offset, results=[TrendingRepo(**row) for row in rows]
    )
