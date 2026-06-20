import gzip
import json
from collections import Counter
from google.cloud import bigquery, storage
from config import BQ_DATASET, BRONZE_BUCKET, GCP_PROJECT

TABLE_NAME = "hourly_event_counts"
TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"

SCHEMA = [
    bigquery.SchemaField("event_hour", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_count", "INTEGER", mode="REQUIRED"),
]

def _read_events(bronze_key: str) -> list[dict]:

    raw = (
        storage.Client(project=GCP_PROJECT)
        .bucket(BRONZE_BUCKET)
        .blob(bronze_key)
        .download_as_bytes()
    )
    
    text = gzip.decompress(raw).decode("utf-8")
    
    # NDJSON records are separated by "\n" only. Do NOT use str.splitlines():
    # it also splits on Unicode line boundaries (U+0085, U+2028, ...) that appear
    # raw inside event text, which would chop a JSON object in half.
    return [json.loads(line) for line in text.split("\n") if line]

def _ensure_table(client: bigquery.Client) -> None:
    
    table = bigquery.Table(TABLE_ID, schema=SCHEMA)
    
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.HOUR,
        field="event_hour",
    )
    
    client.create_table(table, exists_ok=True)

def load_event_counts(date: str, hour: int, bronze_key: str) -> str:
    
    events = _read_events(bronze_key)
    counts = Counter(event["type"] for event in events)
    
    event_hour = f"{date}T{hour:02d}:00:00Z"
    
    rows = [
        {"event_hour": event_hour, "event_type": etype, "event_count": n}
        for etype, n in counts.items()
    ]
    
    client = bigquery.Client(project=GCP_PROJECT)
    _ensure_table(client)
    
    partition = date.replace("-", "") + f"{hour:02d}"
    load_target = f"{TABLE_ID}${partition}"
    
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    load_job = client.load_table_from_json(rows, load_target, job_config=job_config)
    load_job.result()
    
    print(f"Loaded {len(rows)} rows -> {load_target}")
    
    return TABLE_ID

if __name__ == "__main__":
    from ingestion.ingest import bronze_key
    
    date, hour = "2024-01-01", 15
    load_event_counts(date, hour, bronze_key(date, hour))