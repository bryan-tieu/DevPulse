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
- [X] PySpark silver job: explode/dedupe/type the nested JSON → partitioned Parquet → BigQuery; wired into Airflow — _Day 6 ✅ **the Spark job itself**: single-node (`local[*]`) Spark in Docker with the GCS connector reads bronze gz, flattens the envelope, dedupes on `event_id`, casts `created_at`, and writes hour-partitioned Parquet to a new silver GCS bucket with dynamic partition overwrite (idempotent); reconciled vs raw (180,387 → 180,386, 1 dupe) + first pytest. **Day 7 ✅** loaded silver Parquet → BigQuery (`silver_events`, HOUR-partitioned on `created_at`, native `SOURCE_FORMAT=PARQUET` load job into the `$YYYYMMDDHH` decorator); wired the Spark job into the DAG via **DockerOperator** (`wait_for_archive >> ingest >> silver_transform >> load_silver`); retired the `Counter` task + dropped `max_active_runs=1`. Full-chain idempotency re-proven through the DAG (re-run = 12 GCS files + 180,386 BQ rows, steady)._
> 🔐 _Security (carries from Phase 0): this is where the pipeline SA first authenticates. Do **not** mint an SA key — use SA impersonation / workload identity (per the Day 2 ADC decision). Keep Airflow connections & secrets in the secrets backend, never in DAG code or git. (Day 7 still runs as personal ADC; the silver bucket still has no pipeline-SA grant — both land with the SA switch. Day 7 also mounts the **Docker socket** on the Airflow scheduler for the DockerOperator = a host-root surface, accepted for local single-user dev; at scale this is `KubernetesPodOperator`/Dataproc with no socket — see [decisions.md](decisions.md).)_

**Phase 2 · Weeks 4–5 — Warehouse modeling & quality**
- [ ] dbt: staging → star schema (fact_events + dim_repo/actor/date/event_type) → marts; incremental fact; tests + docs — _Day 8 ✅ **dbt bootstrap + first vertical sliver**: dbt runs in its own Docker image (`dbt/`, keyless oauth/ADC — isolated after `dbt-bigquery` broke the pipeline `.venv`); `devpulse_gold` dataset added to Terraform (region-matched). `silver_events` declared as a dbt **source**; `stg_events` staging **view** (recovers `event_date`/`event_hour` from `created_at`, 180,386 rows); `dim_event_type` **table** (15 types, first `ref()`); 7 schema tests (`not_null`/`unique`/`accepted_values`) — `dbt build` green, docs/lineage generated, sqlfluff (dbt templater) clean. **Day 9 ✅** the remaining **dimensions**: added `dbt_utils` (package + lockfile); `dim_repo`/`dim_actor` (**surrogate keys** via `generate_surrogate_key`, **Type-1 latest-wins** via `QUALIFY`, grain 55,245/39,030); `dim_date` (**generated** `date_spine` calendar, 366 days, **smart `YYYYMMDD` key**, 0 bytes scanned); `dbt build` green end-to-end (**PASS=28**); lint clean + fixed the dbt-1.11 `accepted_values` deprecation. **Day 10 ✅** the **center of the star**: `fact_events` as the first **incremental** model (**`merge`** on `unique_key='event_id'`, `partition_by=event_date`, `cluster_by=event_type`, `on_schema_change='fail'`) off `ref('stg_events')` — factless, `event_id` degenerate key, FKs **regenerated deterministically** (no dim join); the deferred **`relationships`** tests (all 4 FKs → dim PKs) + `unique`/`not_null` on the grain; and the first mart **`trending_repos_daily`** (stars = `WatchEvent`/repo/day, `rank()` leaderboard). `dbt build` green (**PASS=48**); **idempotency proven** — 2nd run flips `CREATE TABLE`→`MERGE`, row count steady at 180,386; banked the honest finding that the MERGE scans *more* over one partition (8.5→40.4 MiB — pruning only pays at scale). Lint clean, no new `noqa`. Remaining: the 2 marts (`language_momentum`, `contributor_leaderboard`)._
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

- **Current phase:** Phase 1 complete (batch lakehouse core) → **Phase 2 in progress** (warehouse modeling & quality — dbt).
- **Current day:** [Day 10 — The incremental fact_events + first mart](daily/day-10.md) ✅ complete (incremental `merge` fact + `relationships` tests + `trending_repos_daily`; `dbt build` PASS=48; MERGE idempotency proven)
- **Done:** Day 1 — repo skeleton, Python `.venv`, `.gitignore`, GCP account + billing alerts. Day 2 — Terraform provisions the GCS bronze bucket, BigQuery `devpulse_silver` dataset, and least-privilege pipeline service account (applied & verified). Pipeline SA: `devpulse-pipeline@devpulse-dp2622.iam.gserviceaccount.com`. Day 3 — end-to-end thin slice: `ingestion/` downloads one GH Archive hour → GCS bronze (Hive-partitioned, idempotent via `blob.exists()`), `transform/` counts events by type → BigQuery `hourly_event_counts` (WRITE_TRUNCATE), `api/` serves `/event-counts` over the table. Run as me via ADC; verified end-to-end then `terraform destroy`ed. Day 4 — Airflow in Docker (LocalExecutor): trimmed the official compose stack (no Celery/redis/worker), extended the image with the GCP libs, mounted code + ADC read-only into the containers, and wrapped the unchanged `ingest_hour`/`load_event_counts` in an `@hourly` `ingest >> transform` DAG that derives `date`/`hour` from the run's data interval (`catchup=False`, `retries=2`). Triggered the 2024-01-01 15:00 hour: both tasks green, bronze object + `hourly_event_counts` matched Day 3, and a clear/re-run proved idempotency (`SUM(event_count)` unchanged). Day 5 — made `hourly_event_counts` **HOUR-partitioned** on `event_hour` and switched the load to a partition decorator (`table$YYYYMMDDHH`, `WRITE_TRUNCATE`) so each hour replaces only its own partition (lifting the whole-table truncate that pinned Day 4 to one hour); table created idempotently (`create_table(exists_ok=True)`). Added a `wait_for_archive` PythonSensor (`mode="reschedule"`, HEAD on the GH Archive URL) gating `ingest`, plus `max_active_runs=1` so the in-memory transform can't OOM under backfill fan-out. Ran a bounded `dags backfill -s 2024-01-01 -e 2024-01-03` (48 contiguous hours, 0 failures), verified partition-scoped idempotency (re-loading an hour left the grand total + that hour's sum unchanged), and cleaned up stray live-scheduler runs. Day 6 — the **PySpark silver job**, standalone: a new `spark/` Docker stack runs single-node `local[*]` Spark with the hadoop3 GCS connector (jar `chmod 644`'d so the non-root `spark` user can load it) reading bronze gz via ADC. `silver_events.py` reads with an **explicit schema** (no inferSchema), flattens the envelope (`actor.id`, `repo.name`, …), **dedupes on `event_id`**, casts `created_at`→TIMESTAMP, derives `event_date`/`event_hour`, and writes Hive-partitioned Parquet to a new **silver GCS bucket** (`gs://devpulse-dp2622-silver/events`) with **dynamic partition overwrite** (partition-scoped, idempotent — the lake analog of Day 5's BQ decorator). Hit + fixed an OOM on parallel GCS upload under `local[*]` by baking `spark.driver.memory=4g`. Reconciled 2024-01-01 15:00 against a raw `Counter` (180,387 raw → **180,386** deduped, 1 dupe removed) and proved idempotency (re-run = 12 files, 0 orphaned staging). First **pytest** on the pure `transform_events` (dedupe + malformed-timestamp→NULL) passes in-container. `payload` deliberately dropped from silver for now (varying shape + author-email PII). Day 7 — **the swap**: new `transform/load_silver.py` loads one hour's silver Parquet → BigQuery `silver_events` (HOUR-partitioned on the in-file `created_at` — `partitionBy` strips `event_date`/`event_hour` into the path; malformed-`created_at` rows quarantine in `__HIVE_DEFAULT_PARTITION__` and aren't loaded) via a **free native Parquet load job** into the `$YYYYMMDDHH` decorator (`WRITE_TRUNCATE`); reconciled to 180,386. Wired the Spark job into the DAG as a **DockerOperator** (`silver_transform`) that launches the `devpulse-spark` image on the host daemon over a mounted Docker socket (DooD: host-root surface, host-path injection via `HOST_PROJECT_DIR`/`HOST_ADC`, `mount_tmp_dir=False`), then `load_silver`; **retired the `Counter`** (deprecated `event_counts.py` — the API still reads its table) and **dropped `max_active_runs=1`** (Spark lifts the memory bound it fenced). Proved full-chain idempotency through `dags test` (re-run = 12 GCS files + 180,386 BQ rows, steady). Added `requirements-dev.txt` + a pure-Python `load_silver` unit test (the path/decorator padding asymmetry). Day 8 — **Phase 2 / dbt bootstrap**: stood dbt up in **its own Docker image** (`dbt/Dockerfile` + compose, keyless oauth/ADC) after installing `dbt-bigquery` into the pipeline `.venv` broke it (downgraded `google-cloud-storage`, broke `black`) — recreated the `.venv` clean; isolation is also the production form (dbt later runs as an Airflow DockerOperator + CI image). Added the `devpulse_gold` BQ dataset to **Terraform** (region-matched to silver + pipeline-SA `dataEditor` grant). Scaffolded the dbt project (`dbt_project.yml` with per-layer materializations `staging:view`/`marts:table`, gitignored `profiles.yml`); declared `silver_events` as a dbt **source** (the layer seam); built `stg_events` (staging **view**, recovers `event_date`/`event_hour` from `created_at`, reconciled 180,386) and `dim_event_type` (first **table** + first `ref()`, 15 types); 7 schema tests (`not_null`/`unique`/`accepted_values`) — **`dbt build` green**, `dbt docs` lineage generated, `sqlfluff` (dbt templater) clean. dbt-in-Airflow gate deferred to Phase 2 Week 5. Day 9 — **deepened the star schema's dimensions**: added `dbt_utils` as a dbt **package** (`packages.yml` + committed `package-lock.yml`, pinned 1.4.1; code in gitignored `dbt_packages/`). Built `dim_repo`/`dim_actor` as **tables** off `ref('stg_events')` — md5 **surrogate keys** (`generate_surrogate_key`) beside the natural keys, **Type-1 latest-wins** on the slowly-changing `repo_name`/`actor_login` via `QUALIFY ROW_NUMBER()`; grain reconciled (55,245 repos / 39,030 actors, no fan-out), `dim_actor` holds the PII line (public id+login only, no email). Built `dim_date` as a **generated** `date_spine` calendar (366 days, gap-free, **smart `YYYYMMDD` `date_key`** — the deliberate non-surrogate; 0 bytes scanned). Full `dbt build` green (**PASS=28**), lineage docs regenerated; sqlfluff clean (scoped `noqa` for `date_spine`'s blank lines + the `quarter` identifier) and fixed the Day 8 `accepted_values` dbt-1.11 deprecation (args → `arguments:`). `relationships` tests deferred with the fact. Teardown done (`terraform destroy`, 9 resources). Day 10 — **the center of the star**: built `fact_events` as the first **incremental** model (`merge` on `unique_key='event_id'`, `partition_by=event_date`, `cluster_by=event_type`, `on_schema_change='fail'`) off `ref('stg_events')` — factless, `event_id` the **degenerate** key, four FKs (`repo_sk`/`actor_sk`/`date_key`/`event_type`) **regenerated deterministically** from the natural keys (no dim join — `generate_surrogate_key` is a pure md5 reproducing the dims' keys). Added the deferred **`relationships`** tests (each FK → dim PK, a compiled left-anti-join) + `unique`/`not_null` on the grain — all 10 fact tests pass first try (`dim_date`'s gap-freeness is what lets its `date_key` test pass). Built the first serving mart **`trending_repos_daily`** (table): `stars = count(WatchEvent)` per `(repo, day)`, joined to `dim_repo`, `rank()` leaderboard, grain-guarded by `unique_combination_of_columns`. `dbt build` green (**PASS=48**). **Idempotency proven the incremental way**: a 2nd `dbt run -s fact_events` flipped `CREATE TABLE`→`MERGE`, row count steady at **180,386** (0 inserted — merge on `unique_key` can't duplicate). Banked the honest finding that the MERGE scanned *more* than the full build (8.5→40.4 MiB) — over one partition with a full-hour lookback there's nothing to prune; merge's win is a scale phenomenon. Lint clean, **no new `noqa`**. Teardown done.
- **Next up:** Day 11 — the **remaining two marts**: `language_momentum` (language trend over time, off `fact_events` — note events don't carry language, so this needs a repo→language source, a real modeling decision) and `contributor_leaderboard` (top actors by contribution, `fact_events` → `dim_actor`). Then Phase 2 Week 5's back half: **dbt build as an Airflow DAG gate** + **Great Expectations** bronze→silver. Still pending from Phase 1: pipeline-SA auth (impersonation) + the silver-bucket SA grant; expanding the bounded 48h backfill to a full week on demand.
- **Known issues / blockers:** None blocking. The swap is done — the DAG now runs `ingest >> silver_transform (Spark via DockerOperator) >> load_silver`; the `Counter` is retired from the pipeline but `event_counts.py` + its `hourly_event_counts` table are **deprecated-not-deleted** (still read by `api/main.py` + `run.py`; they retire in the Phase 3 API rework). Still personal ADC — pipeline-SA impersonation + the silver-bucket SA grant remain on the security backlog. The DockerOperator mounts the **Docker socket** (host-root surface) — accepted for local dev; the at-scale fix is `KubernetesPodOperator`/Dataproc (no socket). Cost note: the BigQuery daily byte quota isn't adjustable on this trial account — `maximum_bytes_billed` per query is the planned cap (Phase 3); see [decisions.md](decisions.md).
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
