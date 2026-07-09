# Great Expectations runs in its own image — the Day 8 isolation rule, fourth
# application: GE's heavy dependency tree (pandas, pyarrow, its own pydantic
# pins) must not cohabit with the pipeline .venv, the Airflow image, or the
# Spark image. Same lifecycle as dbt: a compose dev loop now, an Airflow
# DockerOperator task in the DAG.
FROM python:3.11-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The GE project (gx/ context, suites, runner script) is bind-mounted here by
# compose — read-write, like dbt's mount and unlike Spark's: GE writes
# validation results and scratch under gx/uncommitted/.
WORKDIR /opt/devpulse/quality
