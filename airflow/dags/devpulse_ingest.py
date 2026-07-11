from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import requests
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.python import PythonSensor
from alerts import notify_failure
from docker.types import Mount

from airflow import DAG
from ingestion.ingest import GH_ARCHIVE_URL, ingest_hour
from transform.load_silver import load_silver

# Host paths for the Docker-out-of-docker mounts. DockerOperator launches a
# *sibling* container on the host daemon, so the mount SOURCES are resolved by
# the host, not by this scheduler container — hence host paths injected via env
# (HOST_PROJECT_DIR / HOST_ADC), not the scheduler's own /opt/... paths.
HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]
HOST_ADC = os.environ["HOST_ADC"]

# Environment the spark container needs (mirrors spark/docker-compose.yaml),
# read from the scheduler's own env (env_file ../.env) at parse time.
SPARK_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/gcp/adc.json",
    "GCP_PROJECT": os.environ["GCP_PROJECT"],
    "GOOGLE_CLOUD_PROJECT": os.environ["GCP_PROJECT"],
    "BRONZE_BUCKET": os.environ["BRONZE_BUCKET"],
    "SILVER_BUCKET": os.environ["SILVER_BUCKET"],
}

# Environment the dbt container needs (mirrors dbt/docker-compose.yaml's
# environment: block AND its env_file — the operator has no env_file equivalent,
# so everything profiles.yml/_sources.yml env_var() renders must be listed here).
DBT_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/gcp/adc.json",
    "GCP_PROJECT": os.environ["GCP_PROJECT"],
    "GOOGLE_CLOUD_PROJECT": os.environ["GCP_PROJECT"],
    "BQ_DATASET": os.environ["BQ_DATASET"],
}

QUALITY_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/opt/quality/gcp/adc.json",
    "GCP_PROJECT": os.environ["GCP_PROJECT"],
    "GOOGLE_CLOUD_PROJECT": os.environ["GCP_PROJECT"],
    "SILVER_BUCKET": os.environ["SILVER_BUCKET"],
    "BRONZE_BUCKET": os.environ["BRONZE_BUCKET"],
}

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Airflow calls this whenever retries are used up.
    # Flakes are caught through 2 retries.
    # Data quality gates fail after their only attempt.
    "on_failure_callback": notify_failure,
}


def _archive_available(**context) -> bool:
    di = context["data_interval_start"]
    url = GH_ARCHIVE_URL.format(date=di.strftime("%Y-%m-%d"), hour=di.hour)
    return requests.head(url, timeout=30).status_code == 200


def _ingest(**context) -> str:
    di = context["data_interval_start"]
    return ingest_hour(di.strftime("%Y-%m-%d"), di.hour)


def _load_silver(**context) -> str:
    di = context["data_interval_start"]
    return load_silver(di.strftime("%Y-%m-%d"), di.hour)


with DAG(
    dag_id="devpulse_ingest",
    description=(
        "Ingest a GH Archive hour to bronze, Spark-transform to silver, load to "
        "BigQuery, then gate with dbt build (models + tests fail the run on bad data)"
    ),
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    # No max_active_runs cap: Day 5 set it to 1 only because the in-memory Counter
    # couldn't survive backfill fan-out (OOM). The Spark silver job spills to disk
    # and parallelises, so the single-machine bound it fenced is gone — runs may
    # now overlap. (Practical local ceiling is RAM: each run spawns a 4g Spark
    # driver, so a couple concurrent is fine; that's an operational, not a
    # correctness, limit.)
    default_args=default_args,
    tags=["devpulse", "phase1", "phase2"],
) as dag:

    wait_for_archive = PythonSensor(
        task_id="wait_for_archive",
        python_callable=_archive_available,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 6,
    )

    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)

    silver_transform = DockerOperator(
        task_id="silver_transform",
        image="devpulse-spark",
        # Absolute path: spark-submit isn't on the launched container's PATH.
        command=(
            "/opt/spark/bin/spark-submit /opt/devpulse/spark/silver_events.py "
            "{{ data_interval_start.strftime('%Y-%m-%d') }} "
            "{{ data_interval_start.hour }}"
        ),
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/spark",
                target="/opt/devpulse/spark",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=HOST_ADC,
                target="/opt/spark/gcp/adc.json",
                type="bind",
                read_only=True,
            ),
        ],
        environment=SPARK_ENV,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    validate_silver = DockerOperator(
        task_id="validate_silver",
        image="devpulse-quality",
        retries=0,
        command=(
            "python validate_silver.py "
            "{{ data_interval_start.strftime('%Y-%m-%d') }} "
            "{{ data_interval_start.hour }}"
        ),
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/quality",
                target="/opt/devpulse/quality",
                type="bind",
                # Every run results in a write of validation results
                # so RW
            ),
            Mount(
                source=HOST_ADC, 
                target="/opt/quality/gcp/adc.json", 
                type="bind", read_only=True
            ),
        ],
        environment=QUALITY_ENV,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=_load_silver,
    )

    # Retries set to 0. A failing gate catches. Idempotency
    # means that rerunning a failing gate provides the same
    # result, in this case failing again. Save time and resources
    # from rerunning if the result will still fail
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="devpulse-dbt",
        retries=0,
        # Bare `dbt` (on PATH in the image, which bakes no ENTRYPOINT — compose's
        # entrypoint doesn't carry here). No date args, unlike Spark: the build is
        # self-describing — the incremental fact merges from its own watermark.
        command="dbt build",
        mounts=[
            # The DAG's only read-write mount: dbt writes target/ and logs/ into
            # the mounted project dir.
            Mount(
                source=f"{HOST_PROJECT_DIR}/dbt",
                target="/opt/devpulse/dbt",
                type="bind",
            ),
            Mount(
                source=HOST_ADC,
                target="/opt/dbt/gcp/adc.json",
                type="bind",
                read_only=True,
            ),
        ],
        environment=DBT_ENV,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    (
        wait_for_archive
        >> ingest
        >> silver_transform
        >> validate_silver
        >> load_silver_task
        >> dbt_build
    )
