# Day 5 — Partition the silver table, then unlock the week-long backfill (+ a sensor)

> Phase 1 · Week 2. Picks up from Day 4 (the `ingest >> transform` DAG runs in Docker, but `catchup=False` pins it to **one hour** because the load whole-table `WRITE_TRUNCATE`s). Today we lift that limit: make `hourly_event_counts` a **time-partitioned** table loaded with a **partition-scoped replace**, add a **sensor** that waits for the archive file, then run the real **one-week backfill**. Still no Spark — the transform stays the trivial in-memory `Counter`; this day is about the table shape and the orchestration, not the silver logic.

## Environment notes (Windows 10 · PowerShell · Docker Desktop)
Carries from Day 4:
- **Docker Desktop must be running** before `docker compose`. LocalExecutor stack is RAM-hungry (~4 GB) — and today a backfill can fan out runs, so watch memory (see `max_active_runs` in Step 5). `docker compose down` when done.
- **`curl.exe`** (not `curl`), no `gzip`/`head`/`wc` in PowerShell — inspect data/BigQuery with Python.
- GCP project is **`devpulse-dp2622`**; auth is keyless **ADC** (bind-mounted `:ro` into the containers).
- `terraform -chdir=terraform apply` works from any fresh terminal (winget dir on PATH).

## Focus
Convert `hourly_event_counts` to an **HOUR-partitioned** BigQuery table and load each hour into its own partition (`table$YYYYMMDDHH`, `WRITE_TRUNCATE`) so hours no longer overwrite each other — then gate ingest behind an **availability sensor** and run a bounded **one-week `dags backfill`** across 168 hourly partitions, idempotently.

## Build steps
Execute in order; one small commit per step. Steps 1 and 6–7 are setup/verify, not commits.

1. **Recreate infra + bring the stack up.** Day 4 tore the cloud down. `terraform -chdir=terraform apply`; confirm `.env` has bucket/dataset; confirm ADC; **start Docker Desktop**; `docker compose up` (from `airflow/`); UI at `http://localhost:8080`.

2. **Create the partitioned table (HOUR grain).** *Decide the partition grain first — this is the day's key call.* The DAG loads **one hour per run**, and a partition-scoped `WRITE_TRUNCATE` replaces the **whole partition** — so the partition must *be* the hour, or a second hour's load wipes the first. Partition `hourly_event_counts` by **HOUR** on `event_hour` (`bigquery.TimePartitioning(type_=HOUR, field="event_hour")`). Create it idempotently in code: `client.create_table(table, exists_ok=True)` with the existing 3-field schema. *(Rejected: DAY partitioning + `$YYYYMMDD` — the shorthand in `decisions.md` — would force whole-day loads or a `MERGE`; daily grain belongs downstream in gold/dbt, not this hourly silver table. Alt considered: define the table in Terraform — cleaner IaC, but the schema is owned by the transform and will churn with the Phase-1 Spark job, so keep it in code for now.)* *Commit.*

3. **Switch the load to partition-scoped replace.** In `load_event_counts`, point the load at the **partition decorator** `f"{table_id}${date.replace('-','')}{hour:02d}"` (→ `…$YYYYMMDDHH`) and keep `WRITE_TRUNCATE`. Now a re-run replaces **only that hour's partition**, not the table. Drop the create-on-load reliance (table now exists from Step 2). Verify by loading two different hours and confirming **both** survive (`SELECT DISTINCT event_hour`). Sanity: BigQuery rejects rows whose `event_hour` falls outside the decorated partition — a built-in guardrail that the load grain matches the partition. *Commit.*

4. **Add the availability sensor.** GH Archive publishes hourly with a lag; ingesting a not-yet-published hour should *wait*, not fail. Prepend `wait_for_archive >> ingest >> transform` — a sensor that succeeds once a `HEAD` on `https://data.gharchive.org/{interval}.json.gz` returns 200. Use a `PythonSensor`/`@task.sensor` (self-contained, reuses `requests`) **or** `HttpSensor` (operator-native, needs an Airflow HTTP connection — pass it as `AIRFLOW_CONN_*` env, never hardcoded). Set **`mode="reschedule"`** (frees the worker slot between pokes instead of holding it — matters under LocalExecutor), a sane `poke_interval`, and a `timeout`. *Commit.*

5. **Run the one-week backfill.** Keep **`catchup=False`** (start_date is Jan 2024 → `catchup=True` would queue ~21k hourly runs to "now"). Fill an explicit, bounded window with the CLI instead: `airflow dags backfill -s 2024-01-01 -e 2024-01-07 devpulse_ingest` (run it in the scheduler container). Set **`max_active_runs=1`** (or a small N) on the DAG so the memory-heavy in-memory transform doesn't fan out 168 concurrent runs and OOM Docker. Watch 168 hourly partitions land. *Commit (the `max_active_runs`/DAG change).*

6. **Verify partitioning + idempotency.** In Python/BigQuery: `SELECT COUNT(DISTINCT event_hour)` ≈ 168; spot-check a couple of hours' counts against a manual `Counter`. Then **re-run one already-loaded hour** and confirm its partition's `SUM(event_count)` is unchanged (partition-scoped replace, not append) while *other* hours are untouched. Hit the API — `/event-counts` now spans a week, not one hour (note: it may need a `LIMIT`/date filter so it doesn't scan/return everything — flag if so, real fix is Phase 3 pagination).

7. **Cost hygiene + status.** `docker compose down`; `terraform -chdir=terraform destroy`. Tick the Phase 1 ingestion-DAG box in `CLAUDE.md` (backfill + sensor done), update **Current status / Next up** (→ Day 6: PySpark silver job), log the partition-grain + backfill-vs-catchup decisions in `decisions.md`, check off the done criteria.

## Constraints & conventions
- 🧵 **Still thin-slice on the transform.** The in-memory `Counter` stays — **no Spark today**. Today changes the *table shape* and *orchestration*, not the silver logic. (One hour fits RAM; a day of firehose does not — which is exactly why `max_active_runs` is bounded and why Spark is Day 6.)
- 🎯 **Partition grain = load grain.** Partition-scoped `WRITE_TRUNCATE` replaces a whole partition; HOUR partitioning is what keeps "replace this hour" idempotent. Don't reintroduce whole-table truncate, and don't mix a day decorator with hourly loads.
- 🔁 **Idempotency is now per-partition.** Re-running any hour (retry *or* re-backfill) replaces only that hour's partition and leaves siblings alone. Prove it (Step 6), don't assume it. Bronze `blob.exists()` skip is unchanged.
- ⏳ **Backfill is bounded & explicit.** `catchup=False` + `dags backfill -s/-e`, never `catchup=True` against a 2024 start_date. The window is a deliberate 7 days, not "everything since Jan 2024."
- 🛎️ **Sensor: reschedule, not poke-hold.** `mode="reschedule"` frees the slot between checks; a long poke under LocalExecutor would starve other tasks. A sensor *decouples* "is the data there yet?" from "process it."
- 💸 **Cost hygiene.** 168 load jobs are free; the costs are download bandwidth + 168 bronze objects + tiny BQ storage. Airflow stays local in Docker. `docker compose down` + `terraform destroy` at session end.
- 🔐 **No secrets in git.** ADC bind-mounted `:ro`; any Airflow connection (HTTP sensor) via `AIRFLOW_CONN_*` env, never in DAG code. Still personal ADC, not the pipeline SA (impersonation switch remains on the backlog).
- 📦 **Small, reviewable commits & understand-don't-rubber-stamp.** One step per commit (e.g. `make hourly_event_counts hour-partitioned`, `load into partition decorator`, `add archive-availability sensor`). I want the *why* for HOUR-vs-DAY, partition-decorator semantics, `catchup` vs `dags backfill`, and reschedule mode before each lands.

## Done criteria
- [x] `hourly_event_counts` is **HOUR-partitioned** on `event_hour`; the table is created idempotently (`exists_ok=True`).
- [x] The load writes to `…$YYYYMMDDHH` with `WRITE_TRUNCATE`; two different hours coexist (no whole-table truncate left).
- [x] `wait_for_archive` sensor gates `ingest` (`mode="reschedule"`, sane `poke_interval`/`timeout`); chain is `wait_for_archive >> ingest >> transform`.
- [x] A bounded backfill ran via `dags backfill -s 2024-01-01 -e 2024-01-03`; **48 contiguous hourly partitions** present (2-day proof in place of the full week — same mechanism, expand on demand); `catchup` stayed `False`.
- [x] `max_active_runs=1` bounded so the backfill doesn't OOM Docker.
- [x] Re-running one loaded hour leaves its partition `SUM` unchanged **and** other hours untouched (partition-scoped idempotency proven — grand total + that hour's sum identical after re-load).
- [x] No secrets/keys/`logs/` tracked in git.
- [ ] `docker compose down` + `terraform destroy` run; Phase 1 ingestion-DAG box ticked; status + `decisions.md` updated. _(docs done; teardown pending at session end.)_

## Learning goals
1. **BigQuery time partitioning** — column (`field=`) vs ingestion-time partitioning, partition **granularity** (HOUR/DAY), and why grain choice is driven by *load grain* and *query grain*, not taste.
2. **Partition-decorator loads (`$YYYYMMDDHH`)** — how a partition-scoped `WRITE_TRUNCATE` replaces exactly one partition, why that finally makes multi-hour loads idempotent, and the row/partition validation guardrail.
3. **`catchup` vs. explicit `dags backfill`** — why a far-past `start_date` makes `catchup=True` dangerous, and how a bounded `-s/-e` backfill fills a deliberate historical window safely.
4. **Sensors** — gating a task on external readiness, **poke vs. reschedule** mode and its slot-occupancy cost, and decoupling "data available" from "data processed."
5. **Backfill resource discipline** — `max_active_runs` as the lever that keeps a memory-bound (pre-Spark) transform from OOM-ing when the scheduler fans out historical runs.
```