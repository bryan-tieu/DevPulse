# CLAUDE.md — DevPulse

> This file is read automatically at the start of every Claude Code session. It is the operative context for the project. Full design detail lives in `docs/blueprint.md` — read it when you need depth, but this file is the source of truth for goals, conventions, and current status.

---

## Project overview

**DevPulse** is a production-grade data engineering platform that ingests GitHub's global public event stream and transforms it into developer-ecosystem analytics (trending repos, language momentum, contributor leaderboards). It follows a **medallion lakehouse** architecture (bronze → silver → gold) and serves results through a FastAPI service and a dashboard.

**This is a learning project.** Its purpose is to develop genuine, production-level data engineering skills, not just to ship a working app. Optimize for *correct, production-shaped* engineering over quick hacks, and treat me as someone building toward a data engineer role.

---

## How to work with me

- **Teach, don't just do.** Explain *why* before/while you write code — the pattern, the tradeoff, the alternative you rejected. I want to understand every decision well enough to defend it in an interview.
- **Flag production-grade vs. simplified.** When something is simplified for a solo/student setup (e.g. single-node Spark, local Kafka), say so, and tell me what would change at real scale. That articulation is part of what I'm here to learn.
- **Don't silently take shortcuts.** If you skip error handling, idempotency, or tests to move fast, call it out explicitly so it's a conscious choice, not a hidden gap.
- **Prefer small, reviewable changes.** I want to read and understand diffs, not rubber-stamp large rewrites.
- **Ask before large architectural moves.** Use plan mode for anything that spans multiple layers.

---

## Architecture (medallion lakehouse)

Data flows through immutable, increasingly-refined layers:

- **Bronze** — raw, immutable, exactly as ingested. Never transformed. Replayable source of truth. Lives in GCS, partitioned by `date/hour`.
- **Silver** — cleaned, deduplicated, typed, flattened. The heavy transform, done in **PySpark**. Output is partitioned Parquet, loaded into BigQuery.
- **Gold** — business-ready dimensional models (star schema) + aggregate marts, built with **dbt** in BigQuery. This is what the API and dashboard read.

```
GH Archive (hourly .gz) ─┐                                    ┌─► FastAPI (/trending, /languages, /leaderboard)
                         ├─► BRONZE (GCS) ─► SILVER (Spark→BQ) ─► GOLD (dbt star schema + marts) ─┤
GitHub Events API ─► Kafka ┘  (streaming, Phase 4)                                                └─► Dashboard
```

Cross-cutting: Airflow (orchestration), Great Expectations + dbt tests (data quality gates), Terraform (IaC), Docker Compose (local stack), GitHub Actions (CI/CD).

---

## Tech stack & rationale

| Layer | Tool | Why |
|---|---|---|
| Object storage | Google Cloud Storage | Free tier; the bronze/silver lake |
| Warehouse | BigQuery | Serverless, strong free tier, high job demand |
| Orchestration | Apache Airflow (in Docker) | Industry standard; run locally |
| Distributed processing | PySpark (in Docker) | The silver-layer transform |
| Transform / modeling | dbt-core (on BigQuery) | Tests, docs, lineage, dimensional modeling |
| Streaming (Phase 4) | Kafka + Spark Structured Streaming | Real-time ingestion |
| Data quality | Great Expectations + dbt tests | Validation gates between layers |
| IaC | Terraform | Provision GCS + BigQuery + IAM |
| Serving | FastAPI + Streamlit/Looker Studio | Turns the warehouse into a product |
| CI/CD | GitHub Actions | Lint, test, dbt build on PR |

**Cloud is GCP, not AWS** — chosen so the project actually finishes within budget; concepts transfer 1:1 to AWS later.

---

## Repo structure

```
/ingestion    # GH Archive download → GCS bronze; producers
/spark        # PySpark silver-layer jobs
/dbt          # dbt project: staging → dims/facts → marts; tests; docs
/airflow      # DAGs, plugins
/api          # FastAPI service over the gold marts
/terraform    # GCS, BigQuery datasets, IAM
/tests        # pytest unit tests for transform logic
/docs         # blueprint.md and architecture diagram
docker-compose.yml
```

---

## Conventions & standards

- **Python:** `ruff` + `black`; type hints on function signatures; no bare `except`.
- **SQL / dbt:** `sqlfluff`; staging models prefixed `stg_`, dimensions `dim_`, facts `fact_`, marts descriptive (e.g. `trending_repos_daily`). The fact table is **incremental**.
- **Idempotency is mandatory.** Re-running any ingestion or transform must not duplicate or corrupt data. This is a hard rule, not a nice-to-have.
- **Tests:** dbt tests (`not_null`, `unique`, `relationships`, `accepted_values`) on all gold models; pytest for non-trivial Python transform logic; Great Expectations gates bronze→silver.
- **Pipelines fail loudly.** Bad data should break the run with a clear error, never pass silently downstream.
- **Commits:** small, descriptive, imperative mood (e.g. `add idempotent GH Archive ingestion DAG`).

---

## Guardrails (important)

- 💸 **Cost:** A BigQuery billing budget + alert and a byte-scanned quota are set. Partition data and `SELECT` only needed columns — BigQuery bills by bytes scanned. Run `terraform destroy` when not actively working. Keep Spark and Kafka **local in Docker** — no managed clusters.
- 🚫 **Build the ingestion yourself.** GH Archive is also a pre-loaded BigQuery public dataset — do **not** query that to shortcut the pipeline. The point is engineering the ingestion from the raw `.json.gz` files (`https://data.gharchive.org/YYYY-MM-DD-H.json.gz`).
- ✅ **Single-node Spark is intentional.** Don't try to provision a real cluster. When relevant, note what would change at scale.

---

## Milestones (8-week plan)

Core principle: build a thin end-to-end slice first, then deepen each layer. **Weeks 1–6 are the non-negotiable complete project; Phase 4 (streaming) is a stretch — drop it if behind.**

**Phase 0 · Week 1 — Setup & thin vertical slice**
- [X] Repo structure, Docker Compose skeleton, GCP account + billing alerts
- [X] Terraform: GCS bucket + BigQuery dataset + service account
- [X] One file: GH Archive → GCS bronze → trivial transform → one BQ table → one FastAPI endpoint (ugly but end-to-end)

**Phase 1 · Weeks 2–3 — Batch lakehouse core**
- [X] Airflow in Docker; idempotent ingestion DAG → GCS bronze (partitioned); backfill 1 week; sensors + retries — _Day 4 ✅ Airflow (LocalExecutor) + idempotent `ingest >> transform` DAG with retries. Day 5 ✅ hour-partitioned `hourly_event_counts` with partition-scoped load (`table$YYYYMMDDHH`), `wait_for_archive` sensor (reschedule), and a bounded `dags backfill` (48h proof; mechanism proven, expand to full week on demand)._
- [ ] PySpark silver job: explode/dedupe/type the nested JSON → partitioned Parquet → BigQuery; wired into Airflow
> 🔐 _Security (carries from Phase 0): this is where the pipeline SA first authenticates. Do **not** mint an SA key — use SA impersonation / workload identity (per the Day 2 ADC decision). Keep Airflow connections & secrets in the secrets backend, never in DAG code or git._

**Phase 2 · Weeks 4–5 — Warehouse modeling & quality**
- [ ] dbt: staging → star schema (fact_events + dim_repo/actor/date/event_type) → marts; incremental fact; tests + docs
- [ ] Great Expectations bronze→silver gates; dbt tests as a DAG gate; failure alerting; run-metadata logging
> 🔐 _Security (carries from Phase 0): GH Archive payloads contain **author email addresses** (PII). Bronze is locked down, but don't propagate actor emails into the gold dims/marts unless intended — hash or drop them in staging._

**Phase 3 · Week 6 — Serving & CI/CD 🏁 (guaranteed finish line)**
- [ ] FastAPI over gold marts (pagination, caching, auto-docs)
- [ ] Dashboard (Streamlit or Looker Studio)
- [ ] GitHub Actions CI (lint, pytest, dbt build, GE checks); build/push images
> 🔐 _Security (carries from Phase 0) — the slice's API **must be hardened before any public deploy**: (1) **parameterized queries** — never f-string request input into SQL (table/column names from a fixed allowlist only); (2) **auth + rate limiting** — an open endpoint over BigQuery is data exposure **and** billing-DoS (every request runs a paid query job); (3) set **`maximum_bytes_billed`** on every query job; (4) **CORS** — no `allow_origins=["*"]` with credentials; (5) **pin dependencies** + enable Dependabot in CI. Detail in [decisions.md](decisions.md)._

**Phase 4 · Weeks 7–8 — Streaming (stretch)**
- [ ] Kafka in Docker; producer polls Events API → topic; Spark Structured Streaming → bronze/real-time table
- [ ] Real-time mart + endpoint; architecture diagram, README, demo video, "what I'd change at scale" writeup
> 🔐 _Security (carries from Phase 0): the real-time endpoint inherits **all** the Phase 3 API hardening (parameterized queries, auth, rate limiting, `maximum_bytes_billed`). The live Events API also needs a **GitHub token** — store it in the secrets backend, never in the producer code or git._

---

## Current status

> **Update this section at the end of each session.** It's the first thing to read at the start of the next one.

- **Current phase:** Phase 0 complete (thin vertical slice done) → starting Phase 1 (batch lakehouse core).
- **Current day:** [Day 5 — partition the silver table, sensor + backfill](daily/day-05.md) ✅ complete
- **Done:** Day 1 — repo skeleton, Python `.venv`, `.gitignore`, GCP account + billing alerts. Day 2 — Terraform provisions the GCS bronze bucket, BigQuery `devpulse_silver` dataset, and least-privilege pipeline service account (applied & verified). Pipeline SA: `devpulse-pipeline@devpulse-dp2622.iam.gserviceaccount.com`. Day 3 — end-to-end thin slice: `ingestion/` downloads one GH Archive hour → GCS bronze (Hive-partitioned, idempotent via `blob.exists()`), `transform/` counts events by type → BigQuery `hourly_event_counts` (WRITE_TRUNCATE), `api/` serves `/event-counts` over the table. Run as me via ADC; verified end-to-end then `terraform destroy`ed. Day 4 — Airflow in Docker (LocalExecutor): trimmed the official compose stack (no Celery/redis/worker), extended the image with the GCP libs, mounted code + ADC read-only into the containers, and wrapped the unchanged `ingest_hour`/`load_event_counts` in an `@hourly` `ingest >> transform` DAG that derives `date`/`hour` from the run's data interval (`catchup=False`, `retries=2`). Triggered the 2024-01-01 15:00 hour: both tasks green, bronze object + `hourly_event_counts` matched Day 3, and a clear/re-run proved idempotency (`SUM(event_count)` unchanged). Day 5 — made `hourly_event_counts` **HOUR-partitioned** on `event_hour` and switched the load to a partition decorator (`table$YYYYMMDDHH`, `WRITE_TRUNCATE`) so each hour replaces only its own partition (lifting the whole-table truncate that pinned Day 4 to one hour); table created idempotently (`create_table(exists_ok=True)`). Added a `wait_for_archive` PythonSensor (`mode="reschedule"`, HEAD on the GH Archive URL) gating `ingest`, plus `max_active_runs=1` so the in-memory transform can't OOM under backfill fan-out. Ran a bounded `dags backfill -s 2024-01-01 -e 2024-01-03` (48 contiguous hours, 0 failures), verified partition-scoped idempotency (re-loading an hour left the grand total + that hour's sum unchanged), and cleaned up stray live-scheduler runs.
- **Next up:** Day 6 — Phase 1: the **PySpark silver job** — explode/dedupe/type the nested GH Archive JSON → partitioned Parquet → BigQuery, replacing the trivial in-memory `Counter`, wired into Airflow. This removes the single-machine memory limit that forced `max_active_runs=1`.
- **Known issues / blockers:** None. (Whole-table WRITE_TRUNCATE limit **lifted** Day 5 via hour-partitioning. Remaining thin-slice limit: the **in-memory `Counter` transform** — single-machine, memory-bound, capped to `max_active_runs=1`; the Day 6 Spark job removes it. Cost note: the BigQuery daily byte quota isn't adjustable on this trial account — `maximum_bytes_billed` per query is the planned cap (Phase 3); see [decisions.md](decisions.md).)
- **Open decisions:** None outstanding — Day 2–3 choices logged in [decisions.md](decisions.md).

---

## Common commands

> Fill these in as you build each layer.

```bash
# Local stack
docker compose up        # (placeholder) bring up Airflow / Spark / Kafka
# dbt
# cd dbt && dbt build    # run models + tests
# Tests
# pytest                 # python unit tests
# Lint
# ruff check . && black --check . && sqlfluff lint dbt/
# Terraform (cloud infra; run from repo root with -chdir, or cd terraform)
terraform -chdir=terraform init                    # one-time / after provider changes
terraform -chdir=terraform fmt                      # format .tf files
terraform -chdir=terraform validate                 # static check
terraform -chdir=terraform plan                     # preview changes (read-only)
terraform -chdir=terraform apply                    # provision
terraform -chdir=terraform destroy                  # tear down at session end (cost hygiene)
# Auth: uses your ADC (gcloud auth application-default login). Values in terraform/terraform.tfvars (gitignored).
```
