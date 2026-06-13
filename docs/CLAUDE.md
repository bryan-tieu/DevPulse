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
- [ ] Terraform: GCS bucket + BigQuery dataset + service account
- [ ] One file: GH Archive → GCS bronze → trivial transform → one BQ table → one FastAPI endpoint (ugly but end-to-end)

**Phase 1 · Weeks 2–3 — Batch lakehouse core**
- [ ] Airflow in Docker; idempotent ingestion DAG → GCS bronze (partitioned); backfill 1 week; sensors + retries
- [ ] PySpark silver job: explode/dedupe/type the nested JSON → partitioned Parquet → BigQuery; wired into Airflow

**Phase 2 · Weeks 4–5 — Warehouse modeling & quality**
- [ ] dbt: staging → star schema (fact_events + dim_repo/actor/date/event_type) → marts; incremental fact; tests + docs
- [ ] Great Expectations bronze→silver gates; dbt tests as a DAG gate; failure alerting; run-metadata logging

**Phase 3 · Week 6 — Serving & CI/CD 🏁 (guaranteed finish line)**
- [ ] FastAPI over gold marts (pagination, caching, auto-docs)
- [ ] Dashboard (Streamlit or Looker Studio)
- [ ] GitHub Actions CI (lint, pytest, dbt build, GE checks); build/push images

**Phase 4 · Weeks 7–8 — Streaming (stretch)**
- [ ] Kafka in Docker; producer polls Events API → topic; Spark Structured Streaming → bronze/real-time table
- [ ] Real-time mart + endpoint; architecture diagram, README, demo video, "what I'd change at scale" writeup

---

## Current status

> **Update this section at the end of each session.** It's the first thing to read at the start of the next one.

- **Current phase:** Phase 0 — Week 1 (setup & thin vertical slice)
- **Done:** Nothing yet — project just starting.
- **Next up:** Create repo structure; set up GCP account with billing alerts; Terraform a GCS bucket + BigQuery dataset.
- **Known issues / blockers:** None yet.
- **Open decisions:** Confirm data domain (default: GitHub Archive).

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
# Terraform
# cd terraform && terraform plan / apply / destroy
```
