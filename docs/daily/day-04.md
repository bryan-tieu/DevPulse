# Day 4 — Orchestrate the slice with Airflow (Docker → ingestion DAG)

> Phase 1 · Week 2. Picks up from Day 3 (manual end-to-end slice in `run.py`). Today we stand up Airflow in Docker and convert that hand-cranked pipe into a scheduled, retryable **DAG** — wrapping the `ingest_hour` / `load_event_counts` functions we already trust, *not* rewriting them. Spark, sensors, and the week-long backfill come later (Day 5+); today is the orchestration loop.

## Environment notes (Windows 10 · PowerShell · Docker Desktop)
Carries from Day 3, plus Docker:
- **Terraform** is on PATH (winget package dir added to User PATH). `terraform -chdir=terraform apply` works from any fresh terminal.
- **`curl.exe`** (not `curl`), no `gzip`/`head`/`wc` in PowerShell — use Python to inspect data.
- **Docker Desktop must be running** before `docker compose` commands. The Airflow stack (webserver + scheduler + Postgres) is RAM-hungry — expect ~4 GB; close other heavy apps. `docker compose down` when done.
- GCP project is **`devpulse-dp2622`**; auth is keyless **ADC** (the container will read a mounted ADC file).

## Focus
Run the Day 3 pipeline as an Airflow DAG in Docker — one `ingest >> transform` dependency, scheduled and retryable, deriving the target hour from the run's **data interval** — so ingestion becomes orchestrated instead of manual.

## Build steps
Execute in order; one small commit per step. Steps 1 and 6 are setup/verify, not commits.

1. **Recreate infra + prerequisites.** Day 3 destroyed the cloud. `terraform -chdir=terraform apply`; confirm `.env` has the bucket/dataset; confirm ADC (`gcloud auth application-default login`); confirm **Docker Desktop is running**.
2. **Bring up Airflow in Docker.** Create `airflow/` with the official `docker-compose.yaml` pinned to a specific Airflow version, using **LocalExecutor** (solo dev — no Celery/K8s). Add `airflow/dags/`, `airflow/logs/`, `airflow/plugins/`. Init the metadata DB, then `docker compose up`; confirm the UI at `http://localhost:8080`. *Commit.*
3. **Make the project importable + deps available inside Airflow.** The DAG must `import` your `ingestion` / `transform` / `config` modules and have `google-cloud-storage` / `google-cloud-bigquery` present in the image. Mount the repo into the containers (a `volumes:` entry) and set `PYTHONPATH`; add the libs via `_PIP_ADDITIONAL_REQUIREMENTS` (quick) or a small custom `Dockerfile` extending the Airflow image (cleaner). *Commit.*
4. **Wire GCP credentials into the containers.** Mount your local ADC file **read-only** into the container and point `GOOGLE_APPLICATION_CREDENTIALS` (or the well-known path) at it; set `GOOGLE_CLOUD_PROJECT`. Do **not** bake an SA key into the image (security note from `decisions.md`). *Commit.*
5. **Write the DAG.** `airflow/dags/devpulse_ingest.py`: `schedule="@hourly"`, a sane `start_date`, **`catchup=False`** for now. Two `PythonOperator` tasks — `ingest` then `transform` — that derive `date`/`hour` from the run's **data interval** (logical date), then call the *unchanged* `ingest_hour` / `load_event_counts`. Set `retries=2` + `retry_delay`. Declare `ingest >> transform`. *Commit.*
6. **Run + verify in the UI.** Trigger the DAG for one interval. Watch both tasks go green, read the task logs, then confirm — exactly as Day 3 — the bronze object exists and `hourly_event_counts` has rows. Re-trigger the same interval to confirm idempotency (bronze skip, counts unchanged).
7. **Cost hygiene + status.** `docker compose down`; `terraform -chdir=terraform destroy`. Tick the Phase 1 Airflow box in `CLAUDE.md`, log any new choices in `decisions.md`, check off the done criteria.

## Constraints & conventions
- 🧵 **Thin-slice discipline, again.** Orchestrate code you already trust. **No Spark today**, no sensor, no week backfill — those are Day 5+. Get the loop green around the existing functions first.
- ♻️ **Reuse over rewrite.** `ingest_hour` and `load_event_counts` stay *unchanged*; the DAG is a thin wrapper that maps the data interval → their args. If you find yourself editing the transform logic, stop — that's a different day.
- ⚠️ **Known limitation — validate ONE hour/one run only.** The Day 3 load uses whole-table `WRITE_TRUNCATE`, so multiple hours would overwrite each other. That's exactly why `catchup=False` today. The fix (a **time-partitioned BigQuery table**, partition-scoped replace) is Day 5 and is what unlocks the real backfill.
- 🔐 **Least-privilege / no secrets in git.** Mount ADC read-only; never bake a key into the image or commit one. `.gitignore` already covers `*.json`, `.env`; verify `airflow/logs/` and any local Airflow artifacts are ignored too. Keep Airflow connections in the env/secrets backend, not in DAG code.
- 📦 **Small, reviewable commits.** One logical step per commit (e.g. `add airflow docker-compose stack`, `add devpulse ingestion DAG`).
- 🧠 **Understand, don't rubber-stamp.** I want the *why* for the data-interval model, `catchup=False`, LocalExecutor, and how creds reach the container — before each lands.
- 💸 **Cost hygiene.** Airflow runs **local in Docker** (free — no managed Airflow). `docker compose down` + `terraform destroy` at session end. Day 5 starts with `apply` + `compose up` again.

## Done criteria
- [x] Airflow stack runs in Docker; UI reachable at `http://localhost:8080`.
- [x] The DAG appears and **parses with no import errors** (project importable, GCP libs present in the image).
- [x] GCP credentials reach the container — tasks authenticate with no ADC/permission errors.
- [x] One DAG run: `ingest` then `transform` both succeed; bronze object + `hourly_event_counts` rows match the Day 3 manual result.
- [x] Re-running the same interval is idempotent (bronze skipped, counts unchanged).
- [x] `retries`/`retry_delay` set and `ingest >> transform` dependency correct.
- [x] No secrets/keys/`logs/` tracked in git.
- [x] `docker compose down` + `terraform destroy` run; Phase 1 Airflow box ticked in `CLAUDE.md`; choices logged in `decisions.md`.

## Learning goals
1. **Airflow's core model** — DAG / task / operator / scheduler / executor, and the **data-interval (logical date)** concept: a run targets a *time window*, not "now".
2. **Interval-driven, idempotent tasks** — why a task should be a pure function of its data interval, and how that single idea makes retries and backfill safe.
3. **Containerized orchestration wiring** — mounting your code and ADC into Airflow, setting `PYTHONPATH`, and getting project dependencies into the image.
4. **Reuse over rewrite** — wrapping trusted functions in operators instead of reimplementing logic, and why `catchup=False` + whole-table truncate forces a single-hour scope until the partitioned-table upgrade.
