# CLAUDE.md — DevPulse

Auto-loaded every session. This file is the operative context: goals, hard rules, commands, and current status. Deep detail lives in the docs — read them on demand, don't guess:

| Doc | What it holds | Read it when… |
|---|---|---|
| [docs/decisions.md](docs/decisions.md) | Every non-obvious choice + *why* + what changes at scale | Before re-deciding anything; when asked "why did we…" |
| [docs/glossary.md](docs/glossary.md) | Every concept as used here + canonical reference values | A term or project constant is needed |
| [docs/history.md](docs/history.md) | Day-by-day journal of what landed | You need to know how/when something was built |
| [docs/daily/day-NN.md](docs/daily/) | Full plan for each build day | Executing or writing a day plan |
| [docs/DevPulse_Data_Engineering_Blueprint.md](docs/DevPulse_Data_Engineering_Blueprint.md) | The original 8-week design | Planning a new phase |
| [docs/skills-map.md](docs/skills-map.md) | DE job-skill → where it's proven here | Framing work for resume/interviews |
| [docs/tradeoffs.md](docs/tradeoffs.md) | Per-technology honest steelmen: what it buys/costs, **when the alternative wins** | Interview prep; any "defend your stack" question; `/quiz` source |
| [docs/operating-manual.md](docs/operating-manual.md) | Bryan-facing manual for this whole structure | He asks how to use the setup/skills |

---

## What DevPulse is

A **production-grade data engineering learning project**: ingest GitHub's global public event stream (GH Archive hourly `.json.gz`) into a **medallion lakehouse** (bronze GCS → silver PySpark→BigQuery → gold dbt star schema + marts), orchestrated by Airflow, provisioned by Terraform, served by FastAPI + a dashboard.

```
GH Archive (hourly .gz) ─► BRONZE (GCS, date=/hour=) ─► SILVER (Spark → Parquet → BQ) ─► GOLD (dbt star + marts) ─► FastAPI + dashboard
Cross-cutting: Airflow · Terraform · Docker Compose · Great Expectations + dbt tests · GitHub Actions (Phase 3)
```

**The purpose is Bryan's learning, not shipping.** He is self-taught, has no DE work experience yet, and is building toward a data engineer role. Every session must leave him able to defend what was built in an interview.

## Teaching contract (how to work with Bryan)

1. **Teach before/while doing.** For every non-trivial choice: the pattern, the tradeoff, the alternative rejected, and what changes at real scale. Never hand over code he can't explain.
2. **Flag production-grade vs. simplified** explicitly (single-node Spark, seed-instead-of-API, local Airflow…). Articulating the gap *is* the skill.
3. **No silent shortcuts.** Skipped error handling, idempotency, or tests must be called out as a conscious, logged decision.
4. **Small, reviewable changes.** One concept per commit, imperative mood (`add idempotent GH Archive ingestion DAG`). He reads every diff.
5. **Plan mode for anything spanning multiple layers.** Ask before big architectural moves.
6. **Bank the finding.** Surprising results (e.g. "MERGE scanned *more*") go in `decisions.md` honestly — negative results are interview gold.
7. **End every explanation with the interview version**: one or two sentences of how Bryan would say it to an interviewer.

### Coach mode (default from Day 12 onward) — Bryan writes the code

Explanation is not skill. For any step that carries the day's lesson, **Bryan implements; Claude coaches**:

- **Set up, don't solve.** Frame the step (what to build, the pattern, the pitfalls to expect, where to look in existing code/docs), then stop and let Bryan write it. Review his result like a PR from a junior engineer: questions and pointers first — never a silent rewrite.
- **Hint ladder — escalate one level at a time, only when he asks:** (1) concept + where to look → (2) the shape (pseudocode / function signature / model skeleton) → (3) a targeted snippet for the specific stuck line → (4) full solution, **only on explicit request**, and log it in the day log as "solved for me — revisit."
- **Debugging is his rep too.** When something breaks: ask what he observes and what his hypothesis is before explaining. Read the error together; don't translate it instantly.
- **Claude may write directly (not the lesson):** repetitive boilerplate mirroring something Bryan has already built twice (compose plumbing, `.yml` test blocks shaped like existing ones), and repo chores (docs updates, status, teardown). Claude must **not** write the core transform/model/DAG logic the day exists to teach.
- Verification, reconciliation, and lint stay mandatory **regardless of who typed**.

## Session protocol

- **Start of session:** read *Current status* below. If the day needs live data, run `/start-session` (rehydrate runbook). To plan a new build day, run `/plan-day`.
- **During:** follow the active `docs/daily/day-NN.md` plan; one commit per step; prove idempotency + reconcile counts (`/verify-pipeline`) before calling anything done.
- **End of session:** run `/end-session` — it reconciles, lints, updates status/history/decisions, tears down cloud infra, and reminds about commits. **Never end a session leaving `terraform` applied or containers running without saying so.**

## Hard rules (non-negotiable)

1. **Idempotency is mandatory.** Re-running any step must never duplicate or corrupt data. Mechanisms in force: `blob.exists()` skip (bronze), Spark dynamic partition overwrite (lake), BQ partition decorator `table$YYYYMMDDHH` + `WRITE_TRUNCATE` (warehouse), dbt incremental `merge` on `unique_key` (gold). Partition grain = load grain.
2. **Fail loudly, never drop silently.** Explicit schemas (no `inferSchema`/autodetect), `on_schema_change='fail'`, LEFT joins + `COALESCE('Unknown')` over INNER against partial reference data, tests that gate the build. Every aggregate mart must **reconcile** to its upstream (`SUM` = fact `COUNT(*)`).
3. **Build the ingestion yourself.** GH Archive also exists as a BigQuery public dataset — **never** query it as a shortcut. The engineering of the pipeline is the point.
4. **No SA keys, ever.** Keyless ADC (`gcloud auth application-default login`) bind-mounted `:ro` into containers. Pipeline-SA switch happens via impersonation/workload identity, never a minted key. No secrets in git, images, or DAG code.
5. **PII line:** GH Archive payloads carry author emails. `payload` stays out of silver; gold carries `actor_id`/`actor_login` (public) only — never email.
6. **Cost hygiene:** load jobs are free, query jobs bill by bytes scanned (10 MB minimum). Partition + select only needed columns. `terraform destroy` + `docker compose down` at session end. Spark/Kafka stay local in Docker. `maximum_bytes_billed` on every API query job (Phase 3).
7. **`ref()`/`source()` only in dbt** — a literal table name is a bug. Marts read the fact, never staging/source. Single-node Spark is intentional — don't provision a cluster; articulate what would change instead.
8. **Windows/PowerShell environment:** use `curl.exe` not `curl`; no `gzip`/`head`/`wc` in PowerShell; Docker bind sources resolve on the *host* daemon; a missing bind source becomes a silent empty directory.

## Conventions

- **Python:** `ruff` + `black`; type hints on signatures; no bare `except`; pure-transform / I/O split so logic is unit-testable. Dev tooling in `requirements-dev.txt`.
- **SQL/dbt:** `sqlfluff` (BigQuery dialect, dbt templater); `stg_` views, `dim_`/`fact_` and descriptive marts as tables; the fact is incremental. Scoped `noqa` only with a reason.
- **Every new dbt model ships with:** a `.yml` (descriptions feed the docs site), `not_null`/`unique` on its grain, `relationships` on every FK, `unique_combination_of_columns` grain guard on marts, sqlfluff clean, and a reconciliation query proving no rows dropped.
- **Tests:** pytest for non-trivial Python (host `.venv`; Spark tests run in-container and skip on host via `conftest`). dbt tests gate gold. GE gates bronze→silver (Phase 2 back half).

## Reference values (canonical — verify against these)

| Thing | Value |
|---|---|
| GCP project / region | `devpulse-dp2622` / `us-central1` (regional) |
| Buckets | `devpulse-dp2622-bronze`, `devpulse-dp2622-silver` (Parquet at `events/`) |
| BQ datasets | `devpulse_silver` (pipeline-owned) · `devpulse_gold` (dbt-owned) |
| Silver table | `silver_events`, HOUR-partitioned on `created_at` |
| Pipeline SA (exists, not yet used) | `devpulse-pipeline@devpulse-dp2622.iam.gserviceaccount.com` |
| Canonical test hour | `2024-01-01 15:00` → **180,386** silver rows (180,387 raw, 1 dupe) |
| Other canonical counts | dims 55,245 repos / 39,030 actors / 366 dates / 15 types · contributions 163,953 · dbt build **PASS=69** |
| Source URL | `https://data.gharchive.org/YYYY-MM-DD-H.json.gz` |
| Airflow DAG (7 tasks) | `devpulse_ingest`: `wait_for_archive >> ingest >> silver_transform (DockerOperator) >> validate_silver (GE gate, DockerOperator) >> load_silver >> dbt_build (DockerOperator gate) >> record_run_metadata (all_done)` |
| GE counted checks (canonical) | quarantine **0** · raw **180,387** · residual **1** (= the dupe); suite = 8 expectations in `quality/gx/expectations/` |
| Retry routing | `retries=0` on gates (`validate_silver`, `dbt_build`) · `retries=2` elsewhere · `on_failure_callback` in `default_args` (fires after retries exhaust; `upstream_failed` never fires it) |
| Run metadata | `devpulse_silver.pipeline_run_metadata` — unpartitioned, free `load_table_from_json` writes, free `list_rows`/`bq head` reads; summary artifact `quality/run_summary.json` (gitignored) |

## Common commands (run from repo root)

```bash
# ── Cloud infra (personal ADC; values in terraform/terraform.tfvars, gitignored)
terraform -chdir=terraform apply          # provision (9 resources)
terraform -chdir=terraform destroy        # session-end teardown (buckets + gold dataset emptied!)

# ── Rehydrate the canonical hour after a destroy (order matters — see /start-session)
python -c "from ingestion.ingest import ingest_hour; ingest_hour('2024-01-01', 15)"   # host .venv → bronze
docker compose -f spark/docker-compose.yaml up -d --build                              # idle Spark container
docker compose -f spark/docker-compose.yaml exec spark /opt/spark/bin/spark-submit silver_events.py 2024-01-01 15
python -m transform.load_silver                                                        # silver Parquet → BQ (180,386)

# ── dbt (its own image; NEVER install dbt into the pipeline .venv — it broke it once)
docker compose -f dbt/docker-compose.yaml run --rm dbt deps     # after infra recreate
docker compose -f dbt/docker-compose.yaml run --rm dbt build    # models + tests, DAG order (expect PASS=69)
docker compose -f dbt/docker-compose.yaml run --rm dbt docs generate
docker compose -f dbt/docker-compose.yaml run --rm --entrypoint sqlfluff dbt lint models

# ── Airflow (full-chain runs; keep DAG paused during backfills)
docker compose -f airflow/docker-compose.yaml up -d             # webserver on :8080
docker compose -f airflow/docker-compose.yaml down

# ── Tests & lint (host)
python -m pytest tests/                    # Spark tests auto-skip on host; run them in the Spark container
ruff check . && black --check .
```

## Milestones (8-week plan; Weeks 1–6 = the complete project, streaming is a droppable stretch)

- [x] **Phase 0 · Wk 1** — repo, Terraform (GCS + BQ + SA), thin end-to-end slice (Days 1–3)
- [x] **Phase 1 · Wk 2–3** — Airflow ingestion DAG, sensors/retries, 48h backfill (Days 4–5); PySpark silver job → Parquet → BQ, wired via DockerOperator (Days 6–7)
- [x] **Phase 2 · Wk 4–5 (modeling)** — dbt staging → star schema (4 dims + incremental `fact_events`) → 3 marts + seed; 69 tests; docs/lineage (Days 8–11)
- [x] **Phase 2 · Wk 5 (quality)** — dbt gate (Day 12); GE bronze→silver gate (Day 13); failure alerting + retry routing + run-metadata logging, both paths proven (Day 14)
- [ ] **Phase 3 · Wk 6 🏁** — FastAPI over gold marts (pagination, caching); dashboard; GitHub Actions CI (lint, pytest, dbt build, GE). 🔐 Before any deploy: parameterized queries, auth + rate limiting, `maximum_bytes_billed`, deliberate CORS, pinned deps (see decisions.md security backlog). 🔐 Carried from Phase 2: pipeline-SA impersonation + silver-bucket grant.
- [ ] **Phase 4 · Wk 7–8 (stretch)** — Kafka + Spark Structured Streaming; real-time mart; README/diagram/demo video/"at scale" writeup. 🔐 GitHub token in secrets backend only.

Full history of what each day delivered: [docs/history.md](docs/history.md).

## Current status

> Keep this section SHORT (≤ 15 lines). `/end-session` updates it; the narrative goes to `docs/history.md`.

- **Phase:** **Phase 3 in progress** — [Day 15](docs/daily/day-15.md) (FastAPI over the gold marts), **steps 0–4 of 8 done** (s1 2026-07-12→13, s2 07-13, s3 07-14, s4 07-15). Phase 2 complete ✅ (Days 8–14).
- **Day 15 s4 landed** (`1296aa7`+`a17d10e` cache+tests · `5a0ae7b` wiring): hand-rolled `QueryCache` (TTL 300 s, injectable monotonic clock, `KeyError` on absent-or-expired) + cache-aside `cache_run_query` → `(rows, hit)` → **`X-Cache: hit|miss`** on all three mart endpoints. **Live-proven: miss→hit, one BQ job for two identical requests; different date → miss.** Both cache-scoping failure modes hit + fixed live (singleton leaked across tests = 7× 200-where-500; per-request override = cache that can't hit) — banked in decisions.md; isolation = autouse `cache_clear` conftest fixture. s3 `print` reconciliation instruments removed. Suite **62** green; ruff/black clean; **Bryan wrote class + all 12 new tests** (L2 shape hints; zero L4).
- **Next session:** `/start-session` full rehydrate (destroy ran), then Day 15 step 5 (`/runs` via free `list_rows` — no cache, no query job), then 6–8 (retire Phase-0 slice · `/verify-pipeline` · docs: history.md Day-15 entry, skills-map, tradeoffs).
- **Warmups:** item 1 repeat run 07-15 **🔶** (all three 07-08 misses cold at L0 incl. the old L3 upload method; gap: `hour=` partition label; new habit class: post-green edit left the file uncompilable). Due: item 2 ~07-17 (graded on run-after-every-edit), item 3 ~07-18, item 4 second cold pass ≥07-19, item 1 third pass ~07-22, test-file repeat ~07-21 on a **fresh** target.
- **Known issues:** none blocking. Docker engine was off at teardown → stopped Spark container sweeps next `/start-session` (Docker Desktop lives at `AppData\Local\Programs\DockerDesktop`, non-standard). Metadata-DB persistence pattern, 3 members. Observer self-state `"running"` (accepted). Quarantine cleanup manual. `dim_date` static 2024 spine. `transform/event_counts.py` + `run.py` die in step 6. Personal ADC — SA impersonation backlogged. DooD accepted locally. Webhook URL in `.env` (secrets backend = Phase 4).
- **Open decisions:** none — cache rows served **by reference** under a read-only contract (deliberate, comment + `is`-pinned test) and no-maxsize/read-time-eviction accepted at localhost scale; both in [docs/decisions.md](docs/decisions.md).

## Project skills (slash commands)

| Skill | Use when |
|---|---|
| `/start-session` | Rebuild cloud infra + rehydrate the canonical hour so there's data to work against |
| `/plan-day` | Generate the next `docs/daily/day-NN.md` in the house format |
| `/end-session` | Reconcile, lint, update status/history/decisions, tear down, commit checklist |
| `/verify-pipeline` | Prove idempotency + reconcile counts at any layer |
| `/teach <topic>` | Deep-dive a concept the DevPulse way (tradeoffs, at-scale, interview framing) |
| `/quiz` | Interview-prep quiz over everything built so far |
| `/warmup` | Rebuild an existing component cold from a blank file (Days 1–11 were mostly Claude-written; these drills close the typing gap — tracker: `docs/warmups.md`) |
| `/session-analysis` | End-of-session learning retrospective: went well / went badly / review queue / mental models / tradeoffs. Ops close-out stays in `/end-session`; this is the debrief |
