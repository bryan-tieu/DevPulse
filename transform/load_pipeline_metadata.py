from datetime import date, datetime

from google.cloud import bigquery

from config import BQ_DATASET, GCP_PROJECT

TABLE_NAME = "pipeline_run_metadata"
TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"

SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("logical_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("raw_rows", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("hour_rows", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("quarantine_rows", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("residual_rows", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("tasks", "JSON"),
    bigquery.SchemaField("run_summary_json", "JSON"),
]


def build_metadata_row(
    run_id: str,
    recorded_at: str,
    logical_date: str,
    run_summary: dict,
    raw_rows: int,
    hour_rows: int,
    quarantine_rows: int,
    residual_rows: int,
    task_instances: list,
) -> dict:

    return {
        "run_id": run_id,
        "recorded_at": recorded_at,
        "run_summary_json": run_summary,
        "raw_rows": raw_rows,
        "hour_rows": hour_rows,
        "quarantine_rows": quarantine_rows,
        "residual_rows": residual_rows,
        "logical_date": logical_date,
        "tasks": [
            {
                "task_id": task_instance.task_id,
                "state": str(task_instance.state),
                "duration": task_instance.duration,
            }
            for task_instance in task_instances
        ],
    }


def summary_is_fresh(summary: dict, run_start: datetime, logical_date: datetime) -> bool:
    dag_freshness = datetime.fromisoformat(summary["timestamp"]) > run_start

    partition_date_match = date.fromisoformat(summary["partition_date"]) == logical_date.date()
    partition_hour_match = int(summary["partition_hour"]) == logical_date.hour

    return dag_freshness and partition_date_match and partition_hour_match


def _ensure_table(client: bigquery.Client) -> None:

    table = bigquery.Table(TABLE_ID, schema=SCHEMA)

    # No partitioning like we did for load_silver
    # The data being stored is not enough to take
    # advantage of partitioning.

    client.create_table(table, exists_ok=True)


def load_pipeline_metadata(
    run_id: str,
    recorded_at: str,
    logical_date: str,
    run_summary: dict,
    raw_rows: int,
    hour_rows: int,
    quarantine_rows: int,
    residual_rows: int,
    task_instances: list,
) -> str:

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    metadata_row = build_metadata_row(
        run_id,
        recorded_at,
        logical_date,
        run_summary,
        raw_rows,
        hour_rows,
        quarantine_rows,
        residual_rows,
        task_instances,
    )

    client = bigquery.Client(project=GCP_PROJECT)
    _ensure_table(client)

    load_job = client.load_table_from_json([metadata_row], TABLE_ID, job_config=job_config)
    load_job.result()

    print(f"Loaded {load_job.output_rows} row into {TABLE_ID}")

    return TABLE_ID
