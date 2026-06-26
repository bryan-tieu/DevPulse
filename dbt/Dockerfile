# dbt runs in its own image so its heavy dependency tree (pandas, pyarrow,
# google-cloud-aiplatform, its own protobuf/pathspec pins) can't perturb the
# pipeline .venv. This is also the form dbt takes later: an Airflow DockerOperator
# (like the Spark job) and a CI image. Pin the adapter — dbt-core comes with it.
FROM python:3.11-slim

# git: dbt needs it for `dbt deps` (package installs, e.g. dbt_utils for the
# surrogate keys the star-schema dims will use). --no-install-recommends keeps
# the image lean; clean the apt lists in the same layer so they aren't baked in.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Pinned: reproducible builds, no surprise minor bumps between runs.
# sqlfluff + its dbt templater lint the models (the templater resolves ref()/
# source(); the plain jinja one can't). Lives here because linting needs dbt.
RUN pip install --no-cache-dir dbt-bigquery==1.11.3 sqlfluff==3.4.2 sqlfluff-templater-dbt==3.4.2

# The dbt project + profiles live here (bind-mounted by compose, read-write so
# dbt can write target/ and logs/). DBT_PROFILES_DIR points dbt at the in-project
# profiles.yml instead of ~/.dbt.
ENV DBT_PROFILES_DIR=/opt/devpulse/dbt
WORKDIR /opt/devpulse/dbt
