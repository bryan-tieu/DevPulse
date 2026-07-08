from fastapi import FastAPI
from google.cloud import bigquery

from config import BQ_DATASET, GCP_PROJECT

app = FastAPI(
    title="DevPulse",
    description="Developer-ecosystem analytics over the GitHub event stream.",
)

TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.hourly_event_counts"

bq = bigquery.Client(project=GCP_PROJECT)


@app.get("/event-counts")
def event_counts() -> list[dict]:

    sql = f"""
        SELECT event_type, event_count
        FROM `{TABLE}`
        ORDER BY event_count DESC
    """

    rows = bq.query(sql).result()
    return [dict(row) for row in rows]
