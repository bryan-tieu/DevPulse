# Warm-up tracker

> Re-implementation drills (`/warmup`): rebuilding Days 1–11 components cold, from a blank file.
> ✅ cold (twice, ≥1 week apart → retired) · 🔶 warm (repeat scheduled) · 🔁 repeat needed · ⬜ not attempted.
> Grades per the protocol in `.claude/skills/warmup/SKILL.md`. Log every attempt — including the rough ones; the record is the point.

| # | Component | Status | Attempts (date · grade · note) |
|---|---|---|---|
| 1 | `bronze_key` + `ingest_hour` (idempotent GCS ingestion) | 🔶 | 2026-07-08 · 🔶 · structure + idempotency check solid; misses: `hour:02d` zero-pad in partition key, `timeout=` on `requests.get`; L3 hint for the GCS upload method (caps grade). Repeat in ~1 week; focus: key formatting = identity, bounded I/O. |
| 2 | `_hour_partition` + `load_silver` (BQ load job + decorator) | ⬜ | |
| 3 | Spark `SCHEMA` + `transform_events` (flatten/dedupe/cast) | ⬜ | |
| 4 | Spark `run()` + session config (dynamic overwrite) | ⬜ | |
| 5 | Airflow DAG (sensor, interval math, chain, retries) | ⬜ | |
| 6 | `stg_events` + surrogate-key dim with `QUALIFY` | ⬜ | |
| 7 | `dim_date` spine + incremental `fact_events` | ⬜ | |
| 8 | One mart + `.yml` (windows, LEFT-join enrichment, grain guard) | ⬜ | |
| 9 | Terraform core (bucket, dataset, SA, IAM members) | ⬜ | |

**"Solved for me — revisit" queue** (level-4 handouts from build days land here as extra drill targets):

- 2026-07-08 (Day 12): DockerOperator `command=` for the dbt gate — handed the line (`dbt build`) at hint level 3/4 after two conceptual hints didn't land. Drill target: what `command`/`entrypoint`/`Mount(target=)` each mean (what-process-runs vs where-files-appear); folds into ladder item 5 (Airflow DAG).
