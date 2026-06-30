# Design Decisions & Tradeoffs

A running log of non-obvious choices and *why* — interview ammunition and context for future sessions.

---

## Day 2 — Terraform cloud foundation (2026-06-14)

### Terraform state: local
Kept Terraform state local for now since it's mostly solo development — no need for remote state.
Keeping it local avoids standing up a shared remote backend for other developers.
**Production:** remote GCS backend with state locking, so a team shares one source of truth and
concurrent `apply`s can't corrupt state. Migrate later with `terraform init -migrate-state`.

### Credentials: ADC over service-account keys
Keep credentials short-lived to limit the blast radius of any leak. Run Terraform as myself via
ADC (`gcloud auth application-default login`) instead of downloading a long-lived SA key.
The principle: minimize the lifetime and exposure of any credential that can leak — keep the
permanent trust in GCP's IAM system and only ever hand out short-lived tokens. A standalone SA
key collapses both (permanent *and* sitting on disk), which is the single most common GCP leak.
**If a key becomes necessary** (e.g. a container authenticating as the SA), prefer SA impersonation
or workload identity before minting a key.

### GCP APIs enabled outside Terraform
Enabled `storage`, `bigquery`, `iam` via `gcloud services enable`, not `google_project_service`.
APIs are one-time project bootstrap state, not per-deploy infrastructure. Since we `terraform destroy`
daily for cost hygiene, managing APIs in TF would toggle them off/on every cycle. Keeping them out
of the destroy loop is deliberate.

### Region: us-central1 (regional, not multi-region)
Bucket and dataset are co-located in `us-central1`. Regional Standard storage in
us-central1/us-east1/us-west1 qualifies for GCS's always-free 5 GB tier (multi-region `US` does not).
Co-location avoids cross-region scan/egress costs on silver→BQ loads and queries.

### Least-privilege IAM
Pipeline SA gets three narrowly-scoped grants, no `editor`/`owner`:
- `storage.objectAdmin` on the bronze bucket only
- `bigquery.dataEditor` on the silver dataset only
- `bigquery.jobUser` at project level (required — no dataset-scoped "run a job" role exists; grants
  no data access on its own)
Key insight: BigQuery separates *data access* (`dataEditor`) from *job execution* (`jobUser`).
Used `google_*_iam_member` (additive, smallest blast radius) over `_iam_binding`/`_iam_policy`
(authoritative — can clobber existing IAM).

### Dev-only teardown flags
`force_destroy = true` (bucket) and `delete_contents_on_destroy = true` (dataset) let
`terraform destroy` remove non-empty resources for clean daily teardown.
**Production:** both `false`, so Terraform can never delete live data/tables; you'd empty them deliberately.

---

## Day 3 — Thin vertical slice (2026-06-15)

### Hive-style bronze partitioning (`date=.../hour=.../`)
Bronze objects are keyed `date=YYYY-MM-DD/hour=HH/<source-filename>`. The `key=value` directory
convention is what Spark, BigQuery external tables, and dbt recognize for **partition pruning** —
later layers can read just one date instead of scanning the whole lake. The directory hour is
zero-padded for correct lexical sorting; the filename keeps the source's exact name so bronze stays
byte-for-byte traceable to its origin ("exactly as ingested").

### Idempotency: `blob.exists()` skip + `WRITE_TRUNCATE`
Two mechanisms, one rule. Ingestion checks `blob.exists()` and skips — bronze is immutable, so a
re-run is a no-op (and cheaper: no re-download). The BQ load uses `WRITE_TRUNCATE`, so a re-run
**replaces** the table instead of appending. Verified by running the pipe twice and confirming
`SUM(event_count)` was identical, not doubled. `WRITE_APPEND` would have doubled it.
**Limitation (deferred):** `WRITE_TRUNCATE` wipes the *whole* table, so loading a second hour would
erase the first. The fix when Airflow loads many hours is a **time-partitioned table** with the load
scoped to one partition (`table$YYYYMMDD`).

### Trivial in-memory transform as a placeholder for Spark
The silver transform is a Python `collections.Counter` over events in memory — deliberately dumb.
Thin-slice-first: connect the whole pipe before deepening any layer. It does **not** scale (one
small file fits in RAM; a day of the firehose does not) — which is exactly why the real silver layer
is distributed PySpark in Phase 1. Being able to articulate *why* you'd move from this to Spark is
the point.

### `load_table_from_json` (load job) over streaming inserts
Loaded the table with a BigQuery **load job**, which is **free**, rather than streaming inserts,
which cost per row. Cost hygiene baked into the method choice. Also used an **explicit schema with
`mode="REQUIRED"`** (no autodetect) so bad/null data fails the load loudly instead of silently
redefining the table's contract.

### NDJSON parsing: `split("\n")`, never `str.splitlines()`
GH Archive is newline-delimited JSON. `str.splitlines()` also splits on Unicode line boundaries
(`U+0085`, `U+2028`, `U+2029`) that appear **raw inside event text** (commit messages, issue bodies),
which chops a JSON object mid-string → `JSONDecodeError: Unterminated string`. Records are separated
by `\n` only, and any real newline inside a string is escaped, so `text.split("\n")` is correct.
(The Phase 1 Spark JSON reader sidesteps this entirely — another point for moving the transform to Spark.)

### Slice runs as me via ADC, not the pipeline SA
The manual slice authenticates with my own short-lived ADC token (consistent with the Day 2 ADC
decision). The pipeline SA exists for **non-human** callers (Airflow, Spark) arriving in Phase 1;
using it now would mean minting a long-lived key — the exact credential we're avoiding.

---

## Day 4 — Airflow orchestration in Docker (2026-06-16)

### LocalExecutor, not Celery/Kubernetes
Solo dev on one machine, so the DAG runs with **LocalExecutor** — tasks execute as
subprocesses of the scheduler. Celery/K8s executors exist to fan tasks across many worker
nodes via a broker (Redis); with one machine that decoupling is pure overhead, so the
official compose's `redis`/`worker`/`flower` services were removed.
**At scale:** Celery or Kubernetes executor for horizontal task concurrency.

### Tasks are a pure function of the data interval (logical date)
Each task derives `date`/`hour` from `data_interval_start`, never `datetime.now()`. A run
owns a *time window*, not "now" — which is what makes it reproducible, retryable, and (later)
backfillable. The DAG is a thin wrapper mapping interval → the **unchanged** Day 3
`ingest_hour` / `load_event_counts` (reuse over rewrite), passing the bronze key `ingest → transform`
via **XCom**.

### Manual-trigger interval inference = "the window ENDING at the logical date"
Gotcha worth remembering: for the `@hourly` cron timetable,
`infer_manual_data_interval(run_after=L)` returns the last complete window *ending at* `L`.
So to process the **15:00** hour you trigger with logical date **16:00**, not 15:00.
Scheduled runs don't have this (logical_date == data_interval_start); only manual triggers
infer backward. (Verified empirically against the timetable before triggering.)

### catchup=False + whole-table WRITE_TRUNCATE ⇒ one hour only (today)
With `catchup=True` the scheduler would run every hourly interval from `start_date` (Jan 2024)
to now — thousands of runs, each `WRITE_TRUNCATE` clobbering the last. `catchup=False` scopes
today to a single hour. The real backfill waits for Day 5's **time-partitioned BigQuery table**
with a partition-scoped replace (`table$YYYYMMDD`).

### retries=2 is safe *because* the tasks are idempotent
The chain: interval-pure → idempotent (`blob.exists()` bronze skip + `WRITE_TRUNCATE`) →
retries/backfill safe. A retried `ingest` re-skips the existing object; a retried `transform`
re-truncates to the same rows. Verified: clearing + re-running the same interval left
`SUM(event_count)` unchanged (not doubled).

### Credentials: ADC bind-mounted read-only, never baked into the image
The host ADC file is bind-mounted `:ro` into the containers and discovered via
`GOOGLE_APPLICATION_CREDENTIALS`; nothing is `COPY`d into the image (a baked key persists in
image layers and leaks on pull). Still **personal ADC**, not the pipeline SA — consistent with
the Day 3 "runs as me" decision. **Deferred:** switch Airflow to the pipeline SA via
impersonation / workload identity (no minted key) — still on the security backlog below.

### Dependencies: extend the image (committed Dockerfile) over `_PIP_ADDITIONAL_REQUIREMENTS`
A pinned `FROM apache/airflow:2.10.5` + `pip install google-cloud-storage / -bigquery /
python-dotenv` bakes deps once and keeps the list **version-controlled**. `requests` is left to
Airflow's own pin (already present — reinstalling risks a version conflict).
`_PIP_ADDITIONAL_REQUIREMENTS` re-installs on every container boot from a gitignored file —
fine for a quick spike, not reproducible.

### Project reaches the container via mount + PYTHONPATH + env_file
`ingestion/`, `transform/`, `config.py` are mounted read-only to `/opt/devpulse` with
`PYTHONPATH=/opt/devpulse` (imports are top-level). `GCP_PROJECT`/`BRONZE_BUCKET`/`BQ_DATASET`
reach the container via compose `env_file: ../.env` — reuse the existing gitignored config, no
duplication, no values committed.

### Debug note: a missing bind source becomes a silent empty directory
A `gcloud`→`gclouid` typo in the ADC mount source made Docker create an empty *directory* at the
target (Docker doesn't error on a missing bind source — it invents one), surfacing as
`IsADirectoryError`. `docker compose config | grep source` shows the resolved path and finds
such typos fast. Also: argument bugs inside a task callable (e.g. a missing `bronze_key` arg)
pass `dags list-import-errors` because parsing builds the DAG without *calling* the task —
**parse-clean ≠ run-clean.**

---

## Day 5 — Partition the silver table, sensor, bounded backfill (2026-06-19)

### HOUR partitioning + partition decorator (`table$YYYYMMDDHH`), not DAY
`hourly_event_counts` is now **time-partitioned by HOUR on `event_hour`**, and the load targets the
partition decorator `table$YYYYMMDDHH` with `WRITE_TRUNCATE` — so a re-run/new load replaces **only
that hour's partition**, never the whole table. This lifts the Day 3/4 whole-table-truncate limit
that pinned the DAG to one hour. **Grain must match the load grain:** the DAG loads one hour per run,
and a scoped truncate replaces a *whole partition* — so a **DAY** partition (`$YYYYMMDD`, the earlier
shorthand) would let hour 16 wipe hour 15. Daily rollups belong downstream in gold/dbt, not this
hourly silver table. *(Rejected alt: keep DAY partition + a `MERGE`/delete-by-hour — more complex for
no benefit here.)*

### Table created idempotently up front — you can't `ALTER` partitioning in
A partition-decorator load requires the table to **already exist and already be partitioned** (you
can't declare `time_partitioning` on a decorator load). So `_ensure_table` runs
`create_table(table, exists_ok=True)` before every load — idempotent no-op once it exists. Note the
real migration trap: **partitioning is fixed at creation**; changing it is a drop-and-recreate, never
an in-place `ALTER`. Safe here only because the daily `terraform destroy` leaves the dataset empty,
so the first run creates it partitioned from scratch.

### Availability sensor: `PythonSensor`, reschedule mode — and "sensors fail silently into waiting"
`wait_for_archive >> ingest >> transform`. A `PythonSensor` (HEAD on the GH Archive URL, 200 ⇒ ready)
over `HttpSensor` — self-contained, reuses `GH_ARCHIVE_URL`/`requests`, no Airflow HTTP connection to
manage. **`mode="reschedule"`** (not `poke`) releases the worker slot between checks — critical under
LocalExecutor where a poking sensor starves other tasks; rule of thumb: `poke_interval > ~60s ⇒
reschedule`. **Hard lesson:** a `%M` (minutes) vs `%m` (month) typo built `2024-00-01-…` → 404 → the
sensor returned `False` and rescheduled *forever*. A 404 is indistinguishable from "not published
yet," so **a bug in a sensor's check looks identical to legitimately waiting** — sensors fail quietly
into waiting, not loudly into red. Debug the actual artifact, not your mental model of it.

### Backfill: `catchup=False` + explicit `dags backfill -s/-e`, never `catchup=True`
The DAG's `start_date` is Jan 2024; `catchup=True` would queue ~21k hourly runs to "now". Keep
`catchup=False` and fill a **deliberate, bounded window** with the CLI:
`airflow dags backfill -s 2024-01-01 -e 2024-01-03` (48 contiguous hours, proven; `-e` is inclusive
of the 00:00 boundary, hence a 49th run). `catchup` = "run everything I missed"; explicit backfill =
"run *this* window on purpose."

### `max_active_runs=1` — the marker for where the in-memory transform caps out
A backfill wants to fan out all runs at once, but each `transform` loads a whole hour into RAM (the
`Counter`). `max_active_runs=1` serialises them so Docker doesn't OOM. This lever exists **only**
because the transform is single-machine — Day 6's Spark removes the need for it. (Empirically each
backfill run was ~1-2 min, dominated by the GH Archive download, not the load.)

### Operational gotchas banked (Airflow behavior in this version)
- **A paused DAG does NOT run manual triggers** — the run sits `queued` until unpaused (corrected a
  wrong assumption mid-session). Pause stops *all* task scheduling, not just scheduled runs.
- **`airflow tasks test` does not record state in the metadata DB** — it's a deliberately
  side-effect-free dry run (no DagRun advance, no XCom persisted). Great for proving a single task's
  logic; the UI won't show it as green.
- **Live scheduled runs compete with a backfill for the single `max_active_runs` slot.** A current
  hour the scheduler fired (DAG was unpaused) held the slot for ~12 min — the sensor correctly
  *waited* on the not-yet-published file — and blocked the backfill. **Keep the DAG paused during a
  backfill** (`dags backfill` runs regardless of pause); re-pause if it drifts unpaused.

### Cost guardrail finding: BigQuery daily byte quota isn't adjustable here
Tried to set a project-level BigQuery "Query usage per day" quota as a hard cost cap — **not editable
on this trial account** (common: trial/free accounts can't customise it, or it needs the Quota
Administrator role). The real per-query cap is **`maximum_bytes_billed`** on every `QueryJobConfig`
(fails an over-budget query before it scans) — already on the Phase 3 backlog, pulled into focus
here. Not needed for the backfill itself (load jobs are free; only the API's queries scan bytes).
Also: the **billing budget + alert and the byte quota live in the console, not Terraform**, so they're
invisible to the repo and won't survive an account rebuild — codifying them
(`google_billing_budget`, quota override) is a future hardening step.

---

## Day 6 — The PySpark silver job (2026-06-20)

### Scope: build the Spark job *beside* the working `Counter` DAG, swap on Day 7
Day 6 produces silver **Parquet in GCS**, run standalone (`spark-submit`); the BQ load + Airflow
wiring (and retiring the `Counter`/`max_active_runs=1`) are Day 7. Thin-slice rule: don't break the
green DAG mid-Spark-setup — both pipelines coexist until the new one is proven, then swap.

### Single-node `local[*]` Spark in Docker — and what changes at scale
Spark runs in one container as `local[*]` (driver *is* the executor). Intentional — **the code is
identical** to a cluster; only the master URL + auth source change. At scale: Dataproc/EMR with the
connector pre-installed, executors sized (not the driver), distributed writes fanning uploads across
many JVMs. Articulating that is the point, not provisioning a real cluster.

### Reaching GCS from Spark: the connector jar + the perms gotcha
`gs://` is unknown to Hadoop until the **GCS connector jar** is on the classpath. Baked the hadoop3
shaded jar into the image (`/opt/spark/jars/`) rather than `--packages` (reproducible, no per-run
download, no transitive-dep conflicts). **Hard-won:** `ADD <url>` lands the file as `0600 root`; the
image runs as non-root `spark`, which then can't read it → the *generic* `No FileSystem for scheme
"gs"` (not a perms error). Fix: `chmod 644`. Auth reuses **ADC** (`fs.gs.auth.type=APPLICATION_DEFAULT`),
bind-mounted `:ro` — same keyless pattern as Airflow, still personal ADC (not the pipeline SA).

### Explicit schema, drop `payload`
`StructType`, never `inferSchema` — a contract that fails loudly on drift, free column pruning, and
one read instead of two. **Dropped `payload`** from silver: it's a different shape per event type
(no clean single type) *and* it's where the author-email **PII** lives. Per-event-type payload
explosion is the iterative follow-up (Phase 2 staging); omitting it keeps silver narrow and PII-free.

### Pure transform / I/O split → dedupe is a real correctness upgrade
`transform_events(df) -> df` is pure (no read/write); `run()` holds the I/O. That split is what makes
the **first unit test** possible (tiny in-memory DataFrame, no GCS/Java cluster needed beyond a local
session). The transform **dedupes on `event_id`** (`dropDuplicates`) — the `Counter` never did, so
this is genuine correctness, not just a port. Dedupe is a **shuffle** (cheap on one hour; a cost to
note at scale). Reconciled: raw 180,387 → **180,386** silver (1 duplicate event removed).

### Dynamic partition overwrite = the lake analog of Day 5's partition decorator
`partitionBy(event_date, event_hour)` + `mode("overwrite")` **with
`spark.sql.sources.partitionOverwriteMode=dynamic`** replaces only the partitions in the written data.
Default `static` overwrite would **delete the entire `events/` root** every run — the exact whole-table
trap from Day 5, one layer up. Same idempotency rule (partition grain = write grain), enforced in GCS
instead of BigQuery. Proven: re-running an hour left 12 files, **0** orphaned staging.

### The OOM: `driver.memory=4g` must live in conf/CLI, not in-code
In `local[*]` the driver JVM is everything, default heap ~1g. `local[*]` ran ~12 concurrent write
tasks, each buffering a multi-MB resumable GCS upload → `OutOfMemoryError` in the connector's uploader
(read + shuffle were fine; only the **parallel buffered upload** blew up). Fix: `spark.driver.memory
4g`, baked into `spark-defaults.conf`. **Key:** driver memory *cannot* be set via the in-code
`SparkSession.config()` in local mode — the JVM is already launched by the time Python runs; it must
be a `spark-submit` flag or `spark-defaults.conf`.

### Object-store cleanup + commit markers
Failed **dynamic-overwrite** commits leave orphaned `.spark-staging-*` dirs on GCS (no rename-based
cleanup like HDFS) — deleted them manually; worth automating later. Also: dynamic overwrite **doesn't
promote a `_SUCCESS` marker** to the table root (it commits via staging) — `exitCode 0` + "Write Job
committed" is the authoritative signal, and our Day 7 BQ load won't depend on `_SUCCESS`.

### Testing inside the Spark image (no host Java/PySpark)
PySpark tests need Java + the bundled PySpark, which only exist in the container (host is Windows).
Baked `pytest` into the (dev/test) Spark image and mounted `tests/` in; the runner needs
`PYTHONPATH=/opt/spark/python:<py4j zip>` because bare `python3` (unlike `spark-submit`) doesn't add
PySpark to the path. CI provides its own runner in Phase 3.

---

## Day 7 — Silver → BigQuery + wire Spark into Airflow (2026-06-22)

### Native BQ Parquet load job, not the spark-bigquery connector
Silver Parquet → BQ via `load_table_from_uri(source_format=PARQUET)`, not a write from Spark. A load
job is **free** (loads aren't query jobs — no bytes billed) and keeps **Parquet-in-the-lake as the
durable contract** with BQ as a swappable load target. The spark-bigquery connector is the right tool
for streaming / huge writes, but for a free hourly batch it adds a second shaded jar and re-couples the
BQ write into the Spark run. Same `$YYYYMMDDHH` decorator + `WRITE_TRUNCATE` idempotency as Day 5, now
from a Parquet source.

### Partition on `created_at`, not the path's `event_hour`
Spark's `partitionBy("event_date","event_hour")` **strips those columns out of the Parquet files** —
they live only in the GCS path. `created_at` is a real in-file column, so the BQ table is HOUR-
partitioned on it (continues Day 5's HOUR grain), and we load one hour's path prefix into
`silver_events$YYYYMMDDHH`. *(Rejected: BQ hive-partitioning options to recover the path columns — extra
config, and `event_date` is only DAY grain. Derive any date/hour dims in dbt staging instead.)* **Side
effect banked:** malformed-`created_at` rows cast to NULL in Spark → land under
`event_date=__HIVE_DEFAULT_PARTITION__`, so the hour-prefix glob never sees them and the decorator load
can't hit a partition mismatch. They're effectively **quarantined, not loaded** — a real DQ gap to
formalize with the Phase 2 Great Expectations gate (count + alert, don't silently drop).

### Airflow → Spark via DockerOperator over the Docker socket (DooD)
The Spark job lives in a separate stack, so `silver_transform` is a **DockerOperator** that launches a
fresh `devpulse-spark` container on the **host daemon** via a mounted `/var/run/docker.sock`
(Docker-out-of-docker — a *sibling* container, not nested). Three gotchas banked: (1) the socket mount =
**host-root** on the Airflow scheduler (accepted for a local single-user box; documented inline in
compose); (2) **host-path injection** — mount sources are resolved by the host daemon, not the
scheduler, so `HOST_PROJECT_DIR`/`HOST_ADC` are injected as env (forward-slash paths) rather than the
scheduler's `/opt/...`; (3) `mount_tmp_dir=False` — the default tries to bind a host tmp dir and fails
on Windows. Also absolute `/opt/spark/bin/spark-submit` (the launcher isn't on a bare-`exec` PATH).
**At scale this becomes `KubernetesPodOperator` / a Dataproc submit operator** — an authenticated API
call to a scheduler with its own RBAC, no socket; the task ("run this spark-submit") is unchanged, only
the submission transport. *Rejected: `SparkSubmitOperator` (expects a Spark client/cluster in the
Airflow image — mismatched with single-node Docker); a merged Spark+Airflow image (no socket, but
multi-GB and re-couples the stacks Day 6 split); `BashOperator + docker compose run` (same socket,
worse observability).*

### Retire the Counter: deprecate (not delete) + drop `max_active_runs`
The thin-slice **swap**: `silver_transform >> load_silver` replaces the in-memory `Counter` `transform`
task. `max_active_runs=1` is **removed** — it was a *correctness* fence (Day 5) around a memory-bound
transform; Spark spills to disk and parallelises, so the bound it guarded is gone (the new ceiling is
RAM: each run spawns a 4g Spark driver — an *operational*, not correctness, limit). `transform/
event_counts.py` is **deprecated, not deleted**: `api/main.py` still reads its `hourly_event_counts`
table and `run.py` still calls it, so it lives until the Phase 3 API rework moves the API onto
silver/gold. *(The day's outline sanctioned "delete **or clearly deprecate**"; the live API dependency
makes deprecate the correct call — a clean delete would silently break the endpoint for no benefit.)*

### Dev tooling finally tracked: `requirements-dev.txt`
`ruff`/`black`/`pytest` were configured in `pyproject.toml` but never installed or tracked. Added a
`requirements-dev.txt` (kept out of the runtime `requirements.txt`) so the lint/test tooling is
reproducible and CI-ready (Phase 3). `pytest` confined to `tests/` (Airflow's `logs/` symlink is
unstattable on Windows → `WinError 1920`), and the Spark tests skip via `conftest` `collect_ignore` when
PySpark is absent, so the pure-Python `load_silver` test (path/decorator padding asymmetry) runs on the
host venv.

---

## Day 8 — Phase 2 begins: dbt bootstrap (source → staging → first dim) (2026-06-26)

### dbt runs in its own Docker image, never the pipeline `.venv`
Installing `dbt-bigquery` into the shared `.venv` **downgraded `google-cloud-storage` (3.12 → 3.1.1)
and broke `black`** (dbt pins `pathspec` low) — a real, observed conflict, not a hypothetical. dbt's
dependency tree (pandas, pyarrow, `google-cloud-aiplatform`, its own protobuf/pathspec pins) is too
heavy to cohabit with the pipeline runtime. So dbt gets an **isolated image** (`dbt/Dockerfile` +
compose), the `.venv` was recreated clean, and dbt is invoked `docker compose -f dbt/docker-compose.yaml
run --rm dbt <cmd>`. This is also the **production form**: dbt later runs as an Airflow DockerOperator
(exactly like Spark) and a CI image — same per-tool isolation principle as keeping Spark containerized.
*(Rejected: one shared venv with pinned/reconciled versions — dbt and the pipeline deps diverge
indefinitely; you'd fight the resolver forever.)*

### Gold is its own Terraform-managed dataset (`devpulse_gold`), region-matched to silver
Gold gets a **separate BQ dataset** from silver: dbt owns/writes gold, the pipeline owns silver — clean
ownership, access, and cost boundaries, the medallion split mirrored in the warehouse. Provisioned in
**Terraform** (not a dbt-auto-created or `bq mk` dataset) so it's tracked and `terraform destroy` cleans
it up — an ad-hoc dataset would be orphaned state between sessions. **Same `us-central1` as silver** is
load-bearing, not cosmetic: dbt's gold models read silver via `ref()`/`source()`, and a cross-region
read between datasets is a hard BigQuery error. The pipeline-SA `dataEditor` grant on gold lands now
(unused while dbt runs as personal ADC) so the SA-impersonation switch is a one-flip change later.

### Keyless dbt auth + env_var single-source config
`profiles.yml` uses `method: oauth` → resolves the same **ADC** the Spark/ingestion stacks use
(bind-mounted `:ro`, found via `GOOGLE_APPLICATION_CREDENTIALS`); **no SA key minted** (Day 2 rule).
`project`/`schema` come from `env_var('GCP_PROJECT')`/`env_var('BQ_DATASET')` (the `.env`), so dbt's
connection can't drift from the pipeline's. `profiles.yml` is **gitignored** (host/user config; no
secrets under oauth, but kept out of git on principle).

### Materialization per layer: staging=view, marts=table (incremental deferred)
`staging: +materialized: view` — a 1:1 cleaned pass-through; a table would waste storage and go stale
(verified: `CREATE VIEW (0 processed)` — defining a view scans **zero** bytes). `marts: +materialized:
table` — stable, fast for the API/dashboard. `fact_events` flips to **incremental** on a later day (set
per-model, the meaty lesson earns its own day). Set the tool's defaults deliberately; don't let dbt
decide silently.

### `source()`/`ref()` as the contract boundary — the seam IS the architecture
`silver_events` is declared as a dbt **source** in its own step before any model uses it. Staging reads
`{{ source('silver','silver_events') }}`; dims read `{{ ref('stg_events') }}` — **never** a literal
table name. That indirection is what gives dbt the dependency DAG (correct ordering/parallelism) and the
lineage graph. Source `freshness` is declared (warn/error windows) but **not gated today**: against the
fixed 2024-01-01 backfill it would always report stale, and it only runs on `dbt source freshness`, not
`build`/`run` — so it documents production intent without false-failing.

### Staging recovers `event_date`/`event_hour`; `dim_event_type` is the deliberate first dim
`stg_events` re-derives `event_date`/`event_hour` from `created_at` (`DATE()`/`EXTRACT(HOUR …)`, UTC —
matching Spark) because `partitionBy` stripped them into the GCS path and the BQ load never carried them
(the gotcha chained from Day 6 → 7 → here). `dim_event_type` is chosen as the **first** dimension because
`event_type` is its own **natural key** — it exercises the full staging→dim→test loop and the first
`ref()` edge **without** the surrogate-key machinery `dim_repo`/`dim_actor` need (that's Day 9).

### Tests gate the build; `accepted_values` encodes the expected domain
`dbt build` (not bare `run`) interleaves **models and their tests in DAG order** — a broken upstream
fails its test before anything builds on top. `accepted_values` on `event_type` lists the **full
documented GitHub event-type domain** (a superset of the 15 present), so legitimate types from other
hours don't false-fail, but a genuinely unknown type **fails the build** — the loud DQ gate, not a
silent pass. Reconciled: `stg_events` = 180,386 rows (matches silver), `dim_event_type` = 15 types.

### Cost: dbt `build` is bytes-billed query jobs (unlike Day 7's free load), with a 10 MB floor
`dbt run`/`build` issues **query jobs** — bytes-billed, unlike the free Day 7 Parquet *load* job. A
view definition scans 0 bytes, but every query that *reads* hits BigQuery's **10 MB on-demand minimum**
(observed: a `SELECT COUNT(*)` over the one-hour view billed 10 MB). Trivial now; at backfill scale,
staging-as-view re-scans silver on every downstream build — the argument for partition pruning and (for
the fact) incremental. The per-query `maximum_bytes_billed` cap remains the Phase 3 plan.

### dbt is local-only today; Airflow/CI wiring deferred to Phase 2 Week 5
Wiring `dbt build` into the Airflow DAG as a **test gate** (and dbt in CI) is Phase 2 Week 5, not today —
a conscious deferral, flagged so it's not a forgotten gap. The Docker form chosen here is exactly what
that wiring will reuse. `sqlfluff` (+ its **dbt templater**, so `ref()`/`source()` resolve) is baked
into the same image and lints the models clean.

---

## Day 9 — Deepen the star schema: the remaining dimensions (2026-06-29)

### dbt packages: `packages.yml` committed, `dbt_packages/` ignored, lockfile pinned
Added `dbt_utils` via `packages.yml` + `dbt deps`. Same manifest-vs-fetched-code split as
`requirements.txt`/`.venv`: the manifest **and** dbt's generated `package-lock.yml` are committed
(the lock pins the exact resolved version — **1.4.1** — for reproducible builds, like
`.terraform.lock.hcl`), while the downloaded macro code in `dbt_packages/` stays gitignored. Pinned a
**1.x range** (`>=1.1.0, <2.0.0`) compatible with dbt-core 1.11 rather than floating, so a future
breaking major can't bump in silently. `dbt_utils` earns its place for two macros: `generate_surrogate_key`
and `date_spine`.

### Surrogate keys via `generate_surrogate_key` — with the honest tradeoff stated
`dim_repo`/`dim_actor` get md5 **surrogate keys** (`repo_sk`/`actor_sk`) over their natural keys, kept
alongside the natural key (`repo_id`/`actor_id`). The surrogate is the dim PK and the uniform
single-column FK `fact_events` will join on. **Honest caveat (the teaching point):** `repo_id`/`actor_id`
are already clean single-source integers that would serve as keys *as-is* — the surrogate earns its keep
when keys are composite/multi-source or the natural key churns. We adopt the Kimball pattern here to
practise it and give the fact a uniform key shape, **not** out of strict necessity. `dim_event_type`
deliberately stays on its natural key (Day 8) — surrogate keys aren't a reflex.

### Type-1 (overwrite) dimensions via `QUALIFY ROW_NUMBER()`; Type-2 deferred
`repo_name`/`actor_login` are **slowly-changing** (renames) while the ids are stable, so "one row per
entity" needs a rule for *which* name. Keep the **latest** via
`QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1` — a **Type-1** dim (current
value only, no history). `QUALIFY` is BigQuery sugar that filters a window function without a wrapping
subquery. **SCD Type-2** (versioned rows with valid-from/valid-to) is the production alternative, noted
and deferred. Over one hour no rename appears, so the pattern is **exercised, not stress-tested** — flagged
as such. Grain reconciled to silver's distinct counts: `dim_repo` **55,245**, `dim_actor` **39,030** (no
fan-out — the latest-wins collapse is correct), and surrogate `unique` tests pass (no md5 collisions).

### `dim_date` is GENERATED (`date_spine`), not `DISTINCT event_date` — with a SMART key
The date dimension is built from `dbt_utils.date_spine` over a fixed 2024 range (end-exclusive →
**366 rows**, leap year), **not** `SELECT DISTINCT event_date`. A derived calendar would be **gappy** — a
day with zero events = a missing row — which silently breaks date-range joins in the marts. A date dim is
a **conformed** dimension shared by every fact/mart, so it must be complete and event-independent.
**`date_key` is a SMART key** — a readable `YYYYMMDD` integer — the deliberate **exception** to the
"surrogate keys are meaningless" rule (long-standing Kimball convention: human-readable fact rows + cheap
integer range filters), so this dim does **not** use `generate_surrogate_key`. Cost note: `date_spine`
**scans 0 bytes** (pure generation from literals), unlike the dims that read `stg_events`.

### `relationships` tests deferred with the fact (Day 10)
Today's tests are `not_null`/`unique` on **both** the surrogate and natural key of each dim (a surrogate
collision *or* a natural-key dupe both fail loudly). The `relationships` test (FK → dim PK) is the natural
next assertion but is **deferred to Day 10** — it links `fact_events`'s FKs to these dims, and there's no
fact yet. The dims built today are precisely those FK targets.

### Lint hygiene: macro-generated SQL + the dbt-1.11 generic-test deprecation
`date_spine` expands to multi-line generated SQL carrying its own blank lines (sqlfluff **LT15**) that the
source can't reach — scoped with an inline `-- noqa: disable=LT15 / enable=LT15` around the macro call only
(not a global rule drop). `quarter` is flagged **RF04** ("keyword as identifier") by sqlfluff's BigQuery
reserved list even though BigQuery itself allows it as an identifier (and `year`/`month`/`day` aren't
flagged — a quirk of sqlfluff's keyword set) → a one-line `-- noqa: RF04`. Separately, fixed a **dbt-1.11
deprecation** surfaced by the build: generic-test arguments (the Day 8 `accepted_values` on
`dim_event_type`, and today's `accepted_range`) must nest under an **`arguments:`** property — moved them,
deprecation gone.

---

## Security & deployment hardening backlog (deferred from Day 3 / Phase 0)

The thin slice is safe **as built**: localhost only, no untrusted input, no secrets in git, keyless
ADC, least-privilege IAM, bucket public-access-prevention enforced. The items below are **latent** —
patterns that become real vulnerabilities once user input or a public deployment is added. They're
also flagged inline against the relevant phases in `CLAUDE.md`. Goal stated by me: eventually deploy.

- **Parameterized queries (Phase 3).** `api/main.py` builds SQL with an f-string. Safe today (no
  request input), but adding `/languages/{lang}` or pagination params would make it SQL-injectable.
  Use `QueryJobConfig(query_parameters=[...])`. Identifiers (table/column names) can't be
  parameterized — they must come from a fixed allowlist, never from the request.
- **API auth + rate limiting (Phase 3, before any deploy).** The endpoint is unauthenticated. Fine
  on localhost; once on Cloud Run an open endpoint over BigQuery is both data exposure and a
  **billing-DoS** (each request runs a paid query job). Add auth (API key / Cloud Run IAM) + limits.
- **`maximum_bytes_billed` cap (Phase 3).** No per-query cost ceiling today. Set it on every query
  job as defense-in-depth for cost and abuse; pairs with the project byte-scanned quota.
- **CORS (Phase 3).** When the dashboard calls the API, configure CORS deliberately — never
  `allow_origins=["*"]` together with credentials.
- **Dependency pinning (Phase 3 CI).** `requirements.txt` is unpinned (Terraform is pinned + locked
  via `.terraform.lock.hcl`). Pin Python deps via a lockfile (pip-tools/uv) and enable Dependabot to
  close the supply-chain gap.
- **Pipeline SA auth (Phase 1).** When Airflow/Spark first use the pipeline SA, authenticate via
  impersonation / workload identity — do **not** mint a long-lived SA key (consistent with the Day 2
  ADC decision). Keep Airflow connections/secrets in the secrets backend, never in DAG code.
- **PII in bronze (Phase 2).** GH Archive commit payloads include **author email addresses**. Bronze
  is locked down, but don't propagate actor emails into gold dims/marts unless intended — hash or
  drop them in staging.
- **GitHub token for the live Events API (Phase 4).** The streaming producer needs a GitHub token;
  store it in the secrets backend, never in producer code or git.
