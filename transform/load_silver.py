import os

from google.cloud import bigquery

from config import BQ_DATASET, GCP_PROJECT

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
TABLE_NAME = "silver_events"
TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"

# Explicit schema = a contract, not Parquet autodetect. Mirrors the columns the
# Spark silver job emits (spark/silver_events.py).
#
# NOTE: event_date / event_hour are deliberately absent. Spark's partitionBy
# strips them out of the Parquet files and encodes them in the GCS *path*
# (event_date=.../event_hour=15/). We instead partition the table on created_at,
# which is a real column *inside* the files. REQUIRED only on the contract
# columns (id/type/timestamp); a malformed event missing actor/repo should be a
# Phase-2 data-quality finding, not a hard load failure.
SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("actor_id", "INTEGER"),
    bigquery.SchemaField("actor_login", "STRING"),
    bigquery.SchemaField("repo_id", "INTEGER"),
    bigquery.SchemaField("repo_name", "STRING"),
    bigquery.SchemaField("public", "BOOLEAN"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]


def _ensure_table(client: bigquery.Client) -> None:

    table = bigquery.Table(TABLE_ID, schema=SCHEMA)

    # HOUR partitioning on created_at (the in-file timestamp, not the path's
    # event_hour) — continues Day 5's HOUR grain and is what makes the Day-7
    # $YYYYMMDDHH partition-decorator load idempotent. Partitioning can't be
    # ALTERed in after the fact, so it's declared at create time.
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.HOUR,
        field="created_at",
    )

    client.create_table(table, exists_ok=True)


def load_silver(date: str, hour: int) -> str:
    # Glob this hour's Hive partition only. NOTE: event_hour is UNPADDED in the
    # silver path (event_hour=15), unlike bronze's zero-padded hour=15.
    source_uri = f"gs://{SILVER_BUCKET}/events/event_date={date}/event_hour={hour}/*.parquet"

    # Partition-scoped target - Day 5's $YYYYMMDDHH decoration, one layer up.
    partition = date.replace("-", "") + f"{hour:02d}"
    load_target = f"{TABLE_ID}${partition}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace the partition
    )

    client = bigquery.Client(project=GCP_PROJECT)
    _ensure_table(client)
    load_job = client.load_table_from_uri(source_uri, load_target, job_config=job_config)
    load_job.result()  # blocks; raises loudly n schema/partition mismatch

    print(f"Loaded {load_job.output_rows} rows -> {load_target}")
    return TABLE_ID


if __name__ == "__main__":
    load_silver("2024-01-01", 15)
