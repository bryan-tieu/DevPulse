---
name: start-session
description: Rebuild DevPulse cloud infra and rehydrate the canonical data hour after a terraform destroy, so there is data to build against. Use at the start of any session that needs live GCS/BigQuery data (any dbt, Spark, Airflow, or API work).
---

# Start-session: rehydrate the stack

The daily `terraform destroy` empties both buckets and drops the gold dataset. Nothing downstream works until silver is repopulated. **Order matters** — each step feeds the next.

Run from the repo root. Before starting, confirm with the user which parts they actually need today (e.g. pure planning or lint work needs no infra at all — say so and skip).

## Runbook

1. **Preflight (no cost):**
   - `gcloud auth application-default print-access-token` succeeds → ADC is fresh. If not: `gcloud auth application-default login`.
   - `.env` exists at repo root (GCP_PROJECT / BRONZE_BUCKET / SILVER_BUCKET / BQ_DATASET); `terraform/terraform.tfvars` exists. Never print secret values.
   - Docker Desktop is running (`docker info` exits 0).
2. **Provision:** `terraform -chdir=terraform apply` → expect **9 resources** (2 buckets, 2 datasets, SA + IAM grants). Review the plan before approving.
3. **Bronze:** `python -c "from ingestion.ingest import ingest_hour; ingest_hour('2024-01-01', 15)"` (host `.venv`; do **not** use `run.py` — it drives the deprecated Counter path). Idempotent: re-run prints "Already in bronze, skipping".
4. **Silver Parquet (Spark):**
   ```
   docker compose -f spark/docker-compose.yaml up -d --build
   docker compose -f spark/docker-compose.yaml exec spark /opt/spark/bin/spark-submit silver_events.py 2024-01-01 15
   ```
   Success signal = exit 0 + "Write Job committed" (dynamic overwrite writes no `_SUCCESS` marker). Expect **12 Parquet files** under `gs://devpulse-dp2622-silver/events/event_date=2024-01-01/event_hour=15/`.
5. **Silver → BigQuery:** `python -m transform.load_silver` → "Loaded **180,386** rows -> …silver_events$2024010115".
6. **dbt (only if modeling today):**
   ```
   docker compose -f dbt/docker-compose.yaml run --rm dbt deps
   docker compose -f dbt/docker-compose.yaml run --rm dbt build
   ```
   Expect the baseline from CLAUDE.md *Reference values* (currently **PASS=69**). A red baseline before new work means the environment, not the new work, is broken — stop and fix.
7. **Report:** one short summary — what's up, the row count check (180,386 = canonical), and a reminder that `/end-session` handles teardown.

## Rules

- Any count that doesn't match the canonical values in CLAUDE.md is a **stop-and-investigate**, not a shrug.
- If a step fails, debug the actual artifact (URLs, paths, `docker compose config`), not the mental model of it — see decisions.md Day 4/5 for the classic traps (silent empty bind-mount dirs; sensors that wait forever on a typo'd URL).
- Never mint an SA key to "fix" auth. ADC only.
- If the user wants the **full-chain** run instead of the manual chain, use Airflow (`docker compose -f airflow/docker-compose.yaml up -d`, DAG `devpulse_ingest`) — but the manual chain above is faster for a working session.
