from __future__ import annotations 
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from ingestion.ingest import ingest_hour, GH_ARCHIVE_URL
from transform.event_counts import load_event_counts

def _archive_available(**context) -> bool:
    
    di = context["data_interval_start"]
    url = GH_ARCHIVE_URL.format(date=di.strftime("%Y-%m-%d"), hour=di.hour)
    return requests.head(url, timeout=30).status_code == 200
 
def _ingest(**context) -> str:
    
    di = context["data_interval_start"]
    bronze_key = ingest_hour(di.strftime("%Y-%m-%d"), di.hour)
    return bronze_key

def _transform(**context) -> str:
    
    di = context["data_interval_start"]
    bronze_key = context["ti"].xcom_pull(task_ids="ingest")
    return load_event_counts(di.strftime("%Y-%m-%d"), di.hour, bronze_key)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="devpulse_ingest",
    description="Ingest one GH Archive hour to bronze, then load event counts to BigQuery",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["devpulse", "phase1"],
) as dag:
    
    wait_for_archive = PythonSensor(
        task_id="wait_for_archive",
        python_callable=_archive_available,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 6,
    )
    
    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    
    wait_for_archive >> ingest >> transform
    
 
 