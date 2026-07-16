# Glossary — DevPulse

> Definitions of the concepts, tools, and terms used across the project. Scoped to **how they're used here** — each entry gives the general meaning plus the DevPulse-specific role where relevant. Grouped by domain; skim the headings, or Ctrl-F a term. Cross-references in **bold**.

---

## Architecture & the data lake

**Medallion (lakehouse) architecture** — A layered design that refines data through immutable, increasingly-clean stages: **bronze** → **silver** → **gold**. Each layer is a contract the next one builds on. The "lakehouse" part: cheap object-store files (the "lake") plus warehouse-style tables and modeling (the "house") over the same data.

**Bronze layer** — Raw, immutable data exactly as ingested, never transformed — the replayable source of truth. In DevPulse: the hourly GH Archive `.json.gz` files landed in GCS, partitioned `date=…/hour=…/`.

**Silver layer** — Cleaned, deduplicated, typed, flattened data. The heavy transform, done in **PySpark**. In DevPulse: nested JSON events flattened to a flat schema, deduped on `event_id`, written as partitioned **Parquet** and loaded into **BigQuery** (`silver_events`).

**Gold layer** — Business-ready dimensional models (a **star schema**) plus aggregate **marts**, built with **dbt** in BigQuery. This is what the API and dashboard read.

**Data lake** — A store of raw/semi-structured files in cheap object storage (here, GCS), as opposed to a warehouse's managed tables. Bronze and silver Parquet live in the lake; gold lives in the warehouse.

**Data warehouse** — A system optimized for analytical SQL over structured tables (here, **BigQuery**). The medallion split keeps the lake as the durable file contract and the warehouse as a queryable/swappable load target.

**Pipeline** — The end-to-end flow that moves and transforms data: GH Archive → bronze → silver → gold → API. Orchestrated by **Airflow**.

**Idempotency** — Re-running a step produces the same result, never duplicates or corruption. A hard rule in this project. Enforced by: `blob.exists()` skips in ingestion (bronze is immutable), **dynamic partition overwrite** in Spark (lake), and the **partition decorator** + `WRITE_TRUNCATE` in BigQuery (warehouse). The same hour re-run replaces, never appends.

**Partitioning** — Splitting a dataset by a column's value (here, by date/hour) so consumers can read just the slice they need (**partition pruning**) instead of scanning everything. Appears at every layer: Hive-partitioned bronze paths, `partitionBy` Parquet, BigQuery time-partitioned tables.

**Hive partitioning (Hive-style partitioning)** — The `key=value/` directory convention (e.g. `date=2024-01-01/hour=15/`) that Spark, BigQuery, and dbt recognize for **partition pruning**. The "Hive" name comes from Apache Hive, which popularized it. In DevPulse, bronze objects are keyed `date=YYYY-MM-DD/hour=HH/<filename>`; the hour is zero-padded for correct lexical sorting.

**Partition pruning** — A query engine reading only the partitions it needs (e.g. one day) instead of the whole dataset, based on the partition column in the filter. The payoff of partitioning — less data scanned = faster + (in BigQuery) cheaper.

**Backfill** — Running a pipeline over a *past* time window on purpose (e.g. `airflow dags backfill -s 2024-01-01 -e 2024-01-03`). Distinct from **catchup** ("run everything I missed automatically"). Safe here because every task is idempotent and a pure function of its time window.

**Grain** — The level of detail one row represents. "Partition grain = load grain" is a recurring rule: load one hour's data into one hour partition. For a **dimension**, the grain is one row per business entity (one repo, one actor, one day).

---

## Ingestion & file formats

**GH Archive (GitHub Archive)** — A public dataset publishing the global GitHub public event stream as hourly `.json.gz` files at `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`. DevPulse's raw source. (The project deliberately ingests the raw files rather than querying GH Archive's pre-loaded BigQuery copy — the engineering of the ingestion *is* the point.)

**NDJSON (newline-delimited JSON)** — A file format where each line is one independent JSON object. GH Archive files are NDJSON. Parse by splitting on `\n` only — **never** `str.splitlines()`, which also splits on Unicode line boundaries that appear raw inside commit messages/issue bodies and would chop a JSON object mid-string.

**Parquet** — A columnar, compressed, self-describing file format optimized for analytics (column pruning, predicate pushdown). Silver is written as Parquet. Gotcha: Spark's `partitionBy` **strips the partition columns out of the files** — they live only in the directory path, not inside the Parquet.

**`.json.gz` / gzip** — Gzip-compressed JSON. GH Archive's hourly files. Read directly by Spark's JSON reader (it decompresses transparently).

**Envelope** — The common outer structure wrapping every GH Archive event (`id`, `type`, `actor`, `repo`, `created_at`, `payload`, …). The silver transform **flattens** the envelope (`actor.id` → `actor_id`, `repo.name` → `repo_name`, etc.).

**Payload** — The per-event-type nested body of a GitHub event. **Deliberately dropped from silver**: its shape varies by event type (no clean single schema) and it's where author-email **PII** lives. Per-event-type payload modeling is a deferred Phase 2 follow-up.

**Schema (explicit vs. inferred)** — The declared column names + types. DevPulse always uses an **explicit schema** (Spark `StructType`, BigQuery table schema, dbt model contracts) rather than `inferSchema`/autodetect — a contract that fails loudly on drift, prunes columns for free, and avoids a second scan.

**Deduplication (dedupe)** — Removing duplicate rows. Silver dedupes on `event_id` (`dropDuplicates`) — GH Archive occasionally repeats an event. Reconciled: 180,387 raw → 180,386 deduped (1 dupe) for the 2024-01-01 15:00 hour.

---

## Cloud / GCP

**GCP (Google Cloud Platform)** — The cloud provider DevPulse runs on (chosen over AWS for a strong free tier so the project finishes within budget; concepts transfer 1:1). Project id: `devpulse-dp2622`.

**GCS (Google Cloud Storage)** — GCP's object storage. Holds the bronze and silver **lake** (buckets `devpulse-dp2622-bronze`, `devpulse-dp2622-silver`). Qualifies for an always-free 5 GB tier in regional `us-central1`.

**Bucket** — A top-level GCS container for objects. DevPulse has a bronze bucket and a silver bucket.

**Blob (object)** — A single file stored in a GCS bucket (the Python client's `Blob` class). Ingestion calls `blob.exists()` to skip re-uploading an already-landed bronze file — the immutability/idempotency guard.

**BigQuery (BQ)** — GCP's serverless analytics **data warehouse**. Holds silver (`silver_events`) and gold (the dbt models). Billed by **bytes scanned** on queries (not on free **load jobs**). Datasets: `devpulse_silver`, `devpulse_gold`.

**Dataset (BigQuery)** — A namespace grouping tables, scoped to a region. `devpulse_silver` (pipeline-owned) and `devpulse_gold` (dbt-owned) — the medallion split mirrored in the warehouse. Cross-dataset reads require matching regions.

**Region (`us-central1`)** — The single regional location all storage + datasets are co-located in. Regional (not multi-region `US`) qualifies for the free tier and avoids cross-region scan/egress costs; cross-region reads between datasets are a hard BigQuery error.

**Load job (BigQuery)** — A job that ingests files (Parquet/JSON) into a table. **Free** — not bytes-billed, unlike a query job. DevPulse loads silver Parquet → `silver_events` via `load_table_from_uri(source_format=PARQUET)`.

**Query job (BigQuery)** — A job that runs SQL. **Bytes-billed** (10 MB on-demand minimum per query). `dbt run`/`build` issue query jobs; the API's reads do too. A view *definition* scans 0 bytes, but reading through it bills.

**Time partitioning (BigQuery)** — A table physically split by a time column so queries prune to relevant partitions. `silver_events` is **HOUR**-partitioned on `created_at`. Partitioning is fixed at table creation — changing it is a drop-and-recreate, never an in-place `ALTER`.

**Partition decorator (`table$YYYYMMDDHH`)** — BigQuery syntax to load/replace a *single* partition. `silver_events$2024010115` with `WRITE_TRUNCATE` replaces only that hour — the warehouse idempotency primitive. The decorator also fails loudly if a loaded row's timestamp falls outside the partition.

**`WRITE_TRUNCATE` / `WRITE_APPEND`** — BigQuery load dispositions. `WRITE_TRUNCATE` replaces (the target — whole table, or one partition via the decorator); `WRITE_APPEND` adds rows. DevPulse uses `WRITE_TRUNCATE` for idempotent replaces; `WRITE_APPEND` would double data on a re-run.

**`maximum_bytes_billed`** — A per-query cap that fails a query before it scans more than the budget — defense-in-depth against cost/abuse. Planned for Phase 3 (the BigQuery daily byte quota isn't editable on the trial account).

---

## Spark / silver transform

**Apache Spark / PySpark** — A distributed data-processing engine; **PySpark** is its Python API. DevPulse uses it for the heavy silver transform (flatten, dedupe, type). Runs **single-node** here intentionally — the code is identical to a cluster; only the master URL + auth change.

**`local[*]`** — Spark's local master mode: one JVM, the **driver is also the executor**, `*` = use all cores. The single-node setup. At scale this becomes a cluster master (Dataproc/EMR/k8s).

**Driver / executor** — In Spark, the **driver** coordinates the job and holds the plan; **executors** do the distributed work. In `local[*]` they're the same JVM, so driver memory is everything — hence baking `spark.driver.memory=4g` to fix the parallel-upload OOM.

**`StructType` / `StructField`** — Spark's explicit schema types. DevPulse declares the silver schema as a `StructType` rather than inferring it — the contract that fails loudly on drift.

**`spark-submit`** — The CLI that launches a Spark application (`spark-submit silver_events.py 2024-01-01 15`). Driver memory must be set here or in `spark-defaults.conf`, **not** via in-code `SparkSession.config()` — the JVM is already launched by the time Python runs.

**GCS connector (jar)** — The Hadoop library that teaches Spark to read/write `gs://` URIs. Baked into the Spark image (`/opt/spark/jars/`). Gotcha: the jar must be world-readable (`chmod 644`) or the non-root `spark` user can't load it, surfacing as the generic `No FileSystem for scheme "gs"`.

**Dynamic partition overwrite** — `partitionBy(...)` + `mode("overwrite")` with `spark.sql.sources.partitionOverwriteMode=dynamic`: replaces **only the partitions present in the written data**. The lake analog of BigQuery's partition decorator. The default `static` overwrite would delete the entire output root every run — the whole-table-truncate trap, one layer up.

**Shuffle** — A Spark operation that redistributes data across partitions/nodes (e.g. `dropDuplicates`, joins, aggregations). Cheap over one hour; a real cost to note at scale.

**`partitionBy`** — Spark's write option that splits output into `key=value/` directories by column. Strips those columns out of the Parquet files (they live only in the path) — the gotcha that makes BigQuery partition on the in-file `created_at` and dbt re-derive `event_date`/`event_hour`.

**`__HIVE_DEFAULT_PARTITION__`** — The directory Spark writes rows to when the partition column is NULL. Malformed-`created_at` rows (cast to NULL) land here, so the hour-prefix glob never loads them — effectively **quarantined**, a DQ gap to formalize with Great Expectations.

---

## Airflow / orchestration

**Apache Airflow** — The workflow **orchestrator**: schedules, runs, retries, and monitors pipeline tasks as **DAGs**. Runs locally in Docker. Industry-standard, hence chosen.

**DAG (Directed Acyclic Graph)** — Airflow's unit of a workflow: tasks (nodes) with dependencies (edges), no cycles. DevPulse's `devpulse_ingest` DAG (7 tasks): `wait_for_archive >> ingest >> silver_transform >> validate_silver >> load_silver >> dbt_build >> record_run_metadata`.

**Task / Operator** — A DAG node is a **task**, an instance of an **Operator** (a templated unit of work). Used here: **PythonOperator** (run a Python callable), **PythonSensor** (wait for a condition), **DockerOperator** (launch a container).

**LocalExecutor** — Airflow's executor that runs tasks as subprocesses of the scheduler on one machine. Chosen over Celery/Kubernetes executors (which fan tasks across worker nodes via a broker) because it's solo dev on one box — the decoupling would be pure overhead.

**Sensor** — A task that waits for an external condition before downstream tasks run. `wait_for_archive` is a **PythonSensor** doing a HEAD request on the GH Archive URL. Key lesson: **sensors fail silently into waiting** — a bug in the check looks identical to legitimately waiting (a 404 from a date-format typo just reschedules forever).

**`poke` vs. `reschedule` mode** — How a sensor waits. `poke` holds the worker slot the whole time (starves other tasks under LocalExecutor); `reschedule` releases the slot between checks. Rule of thumb: `poke_interval > ~60s ⇒ reschedule`. `wait_for_archive` uses `reschedule`.

**Logical date / data interval** — The *time window* a run owns (`data_interval_start`/`_end`), not "now." Every task derives its `date`/`hour` from the interval, never `datetime.now()` — that's what makes runs reproducible, retryable, and backfillable. Gotcha: for `@hourly`, a *manual* trigger's interval is the window **ending at** the logical date (process 15:00 by triggering with logical date 16:00).

**`catchup`** — If `True`, the scheduler runs every interval from `start_date` to now (thousands of runs). Kept `False`; past windows are filled deliberately via explicit **backfill** instead.

**`max_active_runs`** — A cap on concurrent DAG runs. Was set to `1` (Day 5) only because the in-memory `Counter` transform could OOM under backfill fan-out; **removed** (Day 7) once Spark — which spills to disk — replaced it. The cap came off because Spark *earned* its removal, not as arbitrary cleanup.

**Retries** — Automatic re-runs of a failed task (`retries=2`). Safe **because** the tasks are idempotent — a retried `ingest` re-skips the existing bronze object; a retried load re-truncates to the same rows.

**XCom** — Airflow's cross-task message passing (small values). The `ingest` task passes the bronze object key to the next task via XCom.

**DockerOperator / DooD (Docker-out-of-Docker)** — An Airflow task that launches a **sibling** container on the host Docker daemon via a mounted `/var/run/docker.sock`. `silver_transform` uses it to run the Spark image. Three gotchas: the socket mount = **host-root** on the scheduler (a real surface, accepted for local dev); mount source paths resolve on the *host* daemon (so `HOST_PROJECT_DIR`/`HOST_ADC` are injected); `mount_tmp_dir=False` (the default fails on Windows). At scale → `KubernetesPodOperator`/Dataproc (no socket).

**`on_failure_callback`** — A function Airflow invokes with the task's context when a task *finally* fails — i.e. only **after the retry budget exhausts**. That timing is what makes retry policy compose with alerting: `retries=2` on infra tasks = flakes self-heal silently; `retries=0` on data gates = the first failure pages immediately. In DevPulse: `alerts.py::notify_failure` in `default_args`, POSTing a compact JSON (ids + counts, no event data) to a Discord webhook with a **timeout** (an alert path that can hang the thing it monitors is a bug). `upstream_failed` tasks never execute, so they never fire callbacks — one failure produces **one** page.

**Retry policy by failure class** — Retries exist for *stochastic* failures (network blips, sensor flakes — idempotency makes re-runs free); *deterministic* failures (a data gate re-checking the same bad data) re-fail identically, so retrying only delays the page. Hence `retries=0` on `validate_silver`/`dbt_build`, `retries=2` elsewhere. Day 13→14 measured the difference: ~11 minutes of blind retries vs a same-minute page.

**Trigger rule / `all_done`** — Per-task rule for when it may run, given upstream states. Default `all_success` is why a failed gate stops the pipeline; `trigger_rule="all_done"` (run once upstream *finishes*, in any state) is how an **observer** opts out of dying with the failure it must record. `record_run_metadata` uses it — a metadata row lands even (especially) on red runs.

**`try_number`** — The attempt counter on a task instance. Gotcha: it is **cumulative across clears** (`--reset-dagruns`, manual clear) of the same run_id — an alert reading `Try: 22` reflects a day of re-proofs, not 22 retries. Evidence of `retries=0` working is the absence of retry-gap timestamps, not this field.

**Page fatigue** — The design constraint that the scarcest resource in an alerting system is the recipient's trust: alert on every retry and humans learn to ignore alerts. DevPulse pages on *final* failure only, once per run.

**Run metadata (in-band observer)** — One structured row per pipeline run (`pipeline_run_metadata`: run id, logical date, per-task states/durations, validator counts, composite verdict). Written by a task *inside* the DAG it observes — honest for one DAG, with two measured blind spots: the observer records itself as `"running"` (it can't see its own end), and cleared-but-not-executed tasks carry **stale durations** from prior attempts. At scale observability moves **out-of-band** (Airflow listeners, OpenLineage) so the observer survives scheduler death.

**Logs vs artifacts** — "Logs are for humans, artifacts are for machines": parsing log lines couples consumers to formatting; a structured file (`run_summary.json`) derived from the same verdict booleans gives every consumer (operator, alert, metadata table, future API) one source of truth. The validator's exit code, printed lines, and JSON all derive from one set of booleans — no consumer parses another's text.

---

## dbt / gold modeling

**dbt (data build tool)** — A SQL-first transformation framework that turns `SELECT` statements into managed tables/views, with **tests**, **docs**, and **lineage**. Builds the gold layer in BigQuery. Runs in its own Docker image (isolated after `dbt-bigquery` broke the pipeline `.venv`).

**`dbt_project.yml`** — The project config: name, model paths, and per-layer **materialization** defaults (`staging: view`, `marts: table`). Committed.

**`profiles.yml`** — The warehouse **connection** config (project, dataset, location, auth method). Holds no secrets under OAuth but is **gitignored** on principle (host/user-specific). `dbt debug` is its connectivity smoke test.

**Model** — A dbt `SELECT` in a `.sql` file that dbt materializes as a table or view. DevPulse models: `stg_events`, `dim_event_type`, (Day 9) `dim_repo`/`dim_actor`/`dim_date`, (Day 10) `fact_events` + marts.

**Materialization** — *How* dbt persists a model: **view** (a saved query, 0 storage, always fresh — used for staging), **table** (physical, stable, fast to read — used for dims/marts), **incremental** (append/merge only new rows — coming for `fact_events`). A deliberate per-layer choice, not a silent default.

**Source** — A dbt declaration of an upstream table dbt *reads but doesn't build* (`silver_events`). Models reference it as `{{ source('silver','silver_events') }}` — the **seam** between the silver and gold layers.

**`ref()` / `source()`** — The dbt functions that build the dependency graph. `ref('stg_events')` points at another model; `source(...)` at a declared source. Using them (never a hardcoded table name) is what gives dbt correct ordering, parallelism, lineage, and safe rebuilds — a literal table reference is a bug.

**Staging model (`stg_`)** — The 1:1 "clean and standardize" layer over a source: rename to house style, light casts/derivations, **no joins/aggregation/business logic**. `stg_events` also recovers `event_date`/`event_hour` from `created_at` (the columns `partitionBy` stripped). Materialized as a **view**.

**Dimension (`dim_`)** — A table of descriptive attributes about a business entity (a repo, an actor, an event type, a date) — the "who/what/when/where." Joined to the **fact** in a **star schema**. Grain: one row per entity.

**Fact (`fact_`)** — The central table of measurable events/metrics, with foreign keys to the dimensions. `fact_events` (Day 10) — one row per GitHub event, **incremental**. The "what happened."

**Star schema** — A dimensional model with one central **fact** surrounded by **dimensions** (the shape looks like a star). Optimized for analytical queries and BI. DevPulse's gold layer.

**Surrogate key** — A synthetic, single-column primary key for a dimension (here an md5 hash via `dbt_utils.generate_surrogate_key`), as opposed to the **natural/business key** (e.g. `repo_id`). Gives the fact a uniform single-column FK and decouples the dim from source-key churn. (Honest caveat: a clean single-source integer id often works fine as-is — the surrogate is a Kimball pattern worth practising.)

**Natural / business key** — The real-world identifier a source provides (`repo_id`, `actor_id`, `event_type`). `dim_event_type` uses its natural key directly (no surrogate needed).

**Slowly Changing Dimension (SCD)** — How a dimension handles an attribute that changes over time (a repo rename, an actor login change). **Type-1**: overwrite, keep only the latest (DevPulse's `dim_repo`/`dim_actor`, latest-wins). **Type-2**: keep history as versioned rows with valid-from/valid-to (production alternative, deferred).

**Conformed dimension** — A dimension shared across facts/marts with consistent meaning (e.g. `dim_date`). Generated independently of the fact data so it's complete and **gap-free**.

**Date spine / date dimension** — A pre-generated, contiguous calendar table (`dbt_utils.date_spine`) with one row per day plus attributes (year/month/day-of-week/…). Generated, not `SELECT DISTINCT event_date` (which would be gappy). Uses a **smart key** — a readable `YYYYMMDD` integer — the deliberate exception to "surrogate keys are meaningless."

**Mart** — A gold model shaped for a specific business question / consumer (`trending_repos_daily`, `language_momentum`, `contributor_leaderboard`). What the API and dashboard read. (Deferred to after the fact.)

**dbt tests** — Assertions that gate the build. Mechanically, every test is a **counterexample query**: dbt compiles the YAML to a SELECT returning *violating rows* (visible under `target/compiled/`), and **0 rows = pass**. The five shapes used here: `not_null` (`WHERE col IS NULL`), `unique` (`GROUP BY…HAVING COUNT(*)>1`), `accepted_values` (`DISTINCT col NOT IN (list)`), `relationships` (**left-anti-join** — child key with no parent row; skips NULLs, hence the paired `not_null`), and `dbt_utils.unique_combination_of_columns` (pair-wise grain guard). All are **row-level** — aggregate invariants (mart `SUM` = fact `COUNT`) need singular tests and are currently manual (`/verify-pipeline`). `dbt build` interleaves tests with models in DAG order, failing loudly. `accepted_values` on `event_type` encodes the expected domain — an unknown type fails the build.

**`dbt build` vs. `dbt run`** — `run` builds models only; **`build`** interleaves models **and** tests (and seeds/snapshots) in dependency order — so a broken upstream fails its test before anything builds on top. DevPulse always uses `build`.

**`dbt deps` / package (`packages.yml`)** — dbt's dependency manager. `packages.yml` declares packages (e.g. `dbt_utils`); `dbt deps` fetches them into `dbt_packages/` (gitignored). Manifest committed, fetched code ignored — like `requirements.txt` vs `.venv`.

**dbt_utils** — A community dbt package of helper macros. DevPulse uses `generate_surrogate_key` (hash key) and `date_spine` (calendar generation).

**Source freshness** — A dbt check (`loaded_at_field` + warn/error windows) asserting a source isn't stale. Declared on `silver_events` but **not gated** — it only runs on `dbt source freshness` and would always report stale against the fixed 2024-01-01 dev backfill; it documents production intent.

**Lineage graph** — The dependency DAG dbt auto-generates (`dbt docs`): `silver_events → stg_events → dims/facts → marts`. The visible Phase-2 payoff and a portfolio highlight.

**`QUALIFY`** — BigQuery SQL sugar to filter on a window function's result without a wrapping subquery. Used for the Type-1 latest-wins rule: `QUALIFY ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY created_at DESC) = 1`.

---

## Infrastructure, tooling & quality

**Terraform / IaC (Infrastructure as Code)** — Declaring cloud infrastructure in version-controlled config (`.tf` files) instead of clicking in a console. Provisions DevPulse's GCS buckets, BigQuery datasets, and IAM. `terraform apply` creates, `terraform destroy` tears down (daily, for cost hygiene).

**Terraform state** — Terraform's record of what it manages (mapping config → real resources). Kept **local** here (solo dev); production uses a remote GCS backend with locking so a team shares one source of truth.

**`terraform plan` / `apply` / `destroy`** — Preview changes (read-only) / provision / tear down. The daily `destroy` empties buckets + drops the gold dataset, which is why each session re-hydrates silver before modeling.

**Docker / image / container** — Containerization. An **image** is a built, immutable template; a **container** is a running instance. DevPulse runs Airflow, Spark, and dbt each in their own image — per-tool isolation (the dbt image exists *because* its deps broke the shared `.venv`).

**Docker Compose** — Defines + runs multi-container stacks from a YAML file (`docker-compose.yaml`). Each of Airflow/Spark/dbt has its own compose file. `docker compose down` at session end.

**Bind mount (vs. volume, vs. `COPY`)** — Mapping a **host** path into a container's filesystem at run time (`-v host:container`, compose `volumes:`, DockerOperator `Mount(type="bind")`). The container reads/writes the *same* files as the host — nothing is copied, unlike an image-build `COPY` (baked, immutable) or a named **volume** (Docker-managed storage, no meaningful host path). DevPulse uses bind mounts for all three run-time needs: code the dev loop edits (Spark scripts `:ro`, the dbt project **read-write** — dbt writes `target/`/`logs/`), the ADC credential (`:ro`, never baked — the no-SA-key rule), and the Docker socket (DooD). Two standing traps: mount **sources resolve on the host daemon**, not the calling container (hence `HOST_PROJECT_DIR`/`HOST_ADC` injection in the DAG), and on Docker Desktop a **typo'd/missing source becomes a silently created empty directory**, not an error. At scale bind mounts disappear: code is baked into versioned images; secrets come from workload identity/secret managers.

**Docker socket (`/var/run/docker.sock`)** — The Unix socket the Docker daemon listens on. Mounting it into a container grants **host-root-equivalent** control — the surface the `silver_transform` DockerOperator accepts for local dev (at scale, replaced by an authenticated k8s/Dataproc API call).

**FastAPI** — The Python web framework serving the gold marts as HTTP endpoints (`/trending`, `/languages/momentum`, `/leaderboard`, `/runs`). The Phase 3 rework began Day 15: a parameterized, cost-capped query layer (`api/queries.py`) with a pure-builder/executor split; the endpoints + the death of the old f-string `/event-counts` follow in step 2.

**Dependency injection (FastAPI `Depends`)** — Declaring a function's needs in its **signature** and letting the framework construct and pass them at call time, instead of building them in the body. DevPulse: endpoints declare `client: bigquery.Client = Depends(get_bq_client)`; the provider is an `lru_cache(maxsize=1)` singleton (one ADC handshake per process). The payoff is the test seam: `app.dependency_overrides[get_bq_client] = lambda: mock` — keyed by the original function object, checked *before* the provider runs, so endpoint tests exercise real routing/validation/error handling with **zero GCP** (must be a callable, and must be `clear()`ed in teardown or it leaks into later tests). Standing traps: a body that **rebuilds** what it was injected (the Day-15 shadow-binding bug — tests silently routed to production BQ), and helpers that call the provider directly, bypassing the graph. Also the at-scale migration seam: serving-DB swap or SA impersonation changes the provider, never the endpoints. The API-layer twin of the pure/I-O split.

**Great Expectations (GE)** — A data-quality framework for validation **gates** between layers (planned bronze→silver). Where the quarantined malformed-`created_at` rows get a count-and-alert instead of a silent drop.

**Data quality (DQ) gate** — A validation checkpoint that **fails the run loudly** on bad data rather than passing it silently downstream. Since Day 12, dbt tests gate gold **in-pipeline**: the `dbt_build` DockerOperator ends `devpulse_ingest`, so a failing test fails the DAG run (proven live — an out-of-spine hour turned 4 `relationships` tests red and stopped the run). GE will gate bronze→silver (Day 13).

**CI/CD (GitHub Actions)** — Continuous Integration/Delivery: on each PR, run lint + tests + `dbt build` + GE checks, and build/push images. Phase 3.

**ruff / black** — Python linter (`ruff`) and formatter (`black`). Plus type hints on signatures and no bare `except`. Tracked via `requirements-dev.txt`.

**sqlfluff** — A SQL linter/formatter (BigQuery dialect + the **dbt templater**, so `ref()`/`source()`/macros resolve before linting). Keeps the dbt models clean.

**pytest** — Python's test framework. Covers non-trivial transform logic — the pure `transform_events` (Spark) and the `load_silver` path/decorator derivation. Pure-function / I/O split is what makes the units testable without a cluster.

---

## API & serving

**Cache-aside** — The caching pattern where the *caller* owns the flow: try the cache; on miss, fetch from the source, store, serve. DevPulse: `cache_run_query()` in `api/cache.py` — a shared helper returning `(rows, hit)` so the hit/miss fact is born where the `X-Cache: hit|miss` response header is set (a fact a caller needs must be returned or observable, never swallowed by a wrapper). Contrast: read-through, where the cache itself fetches.

**TTL (time-to-live) cache** — A cache whose entries expire after a fixed age. **The TTL is a freshness policy, not an optimization**: 300 s encodes "answers may be up to 5 min stale." For batch-updated marts the *honest* invalidation key is the latest pipeline `run_id` (data changes exactly when a run lands) — TTL is the accepted approximation. Contrast **LRU** (`functools.lru_cache`): evicts on **capacity**, has no clock — the hottest key is always most-recently-used, so the most popular answer goes stale *forever* (fine for the BQ-client singleton, which never ages; wrong for query results). Real caches compose both: freshness *and* memory are separate policies (Redis `maxmemory` + per-key TTL). DevPulse's is in-process = **per-worker** (two uvicorn workers = two caches) and unbounded with read-time-only eviction (abandoned keys never reclaimed) — named, accepted at localhost scale.

**Monotonic clock (`time.monotonic`)** — A clock that only moves forward, unlike the wall clock (`time.time`), which NTP can step backwards. Cache expiry math on a wall clock silently *extends* entries past their TTL on a back-step (policy violation); a forward jump merely expires early (pennies). Injected as a constructor argument so tests control time with a fake clock — no `sleep()`.

**Singleton vs per-request scope (DI)** — What a `Depends` provider returns per call. `@lru_cache` on the provider = one shared instance (required for a cache: a fresh-per-request cache structurally cannot hit — nothing errors, `X-Cache: hit` just never appears). The flip side in tests: a session-wide singleton leaks state *between tests* — DevPulse resets it with an autouse `conftest.py` fixture (`get_query_cache.cache_clear()`), chosen over per-test overrides so isolation can't be forgotten in the next test file. Both failure modes were hit live on Day 15 s4.

## Security & credentials

**ADC (Application Default Credentials)** — Google's mechanism for discovering credentials from the environment (`gcloud auth application-default login` → a short-lived user token). DevPulse authenticates **keyless** via ADC everywhere (Terraform, Spark, Airflow, dbt), bind-mounted read-only into containers — **no SA key on disk**.

**Service Account (SA)** — A non-human identity for machines/services. DevPulse's pipeline SA (`devpulse-pipeline@…`) exists with **least-privilege** grants but isn't used yet — the pipeline still runs as personal ADC; switching to the SA via **impersonation** (not a minted key) is on the backlog.

**SA key** — A long-lived credential file for a service account. **Deliberately never minted** — it's permanent *and* sits on disk, the single most common GCP leak. Prefer **impersonation** / **workload identity** (short-lived tokens) instead. (The Day 2 ADC decision.)

**SA impersonation / workload identity** — Ways for a service to *act as* an SA using short-lived tokens, without a downloaded key. The planned path for switching the pipeline off personal ADC.

**OAuth (dbt `method: oauth`)** — dbt's auth method that resolves the same ADC the rest of the project uses — keyless, no SA key.

**Least privilege** — Granting only the minimal permissions needed. The pipeline SA gets three narrow grants (`storage.objectAdmin` on bronze, `bigquery.dataEditor` on silver, project-level `bigquery.jobUser`), never `editor`/`owner`. Note: BigQuery separates **data access** (`dataEditor`) from **job execution** (`jobUser`).

**IAM (Identity and Access Management)** — GCP's permission system. DevPulse uses additive `google_*_iam_member` grants (smallest blast radius) over authoritative `_iam_binding`/`_iam_policy` (which can clobber existing IAM).

**PII (Personally Identifiable Information)** — Here, the **author email addresses** in GH Archive commit payloads. Bronze is locked down; silver **drops `payload`** entirely; gold keeps actor identity to `actor_id`/`actor_login` (public username) only — never email. A boundary re-asserted at `dim_actor`.

**Parameterized queries** — Passing user input to SQL as bound **parameters** (`QueryJobConfig(query_parameters=…)`), never f-string interpolation — the fix for SQL injection: the value travels out-of-band and never enters the SQL text. Identifiers (table/column names) can't be parameterized in any engine; they come only from config constants / a fixed **allowlist**. Live since Day 15 (`api/queries.py` — every value a `ScalarQueryParameter` with an **explicit type**; autodetect is the `inferSchema` trap).

**`maximum_bytes_billed`** — A per-query-job cost ceiling (`QueryJobConfig`): BigQuery **fails the job before scanning** if its estimate exceeds the budget. Contrast: billing alerts fire after the fact; IAM/project quotas are blunt. DevPulse: 100 MB on every API query job (a tripwire, not a constraint — the mart queries dry-run at KBs–MBs), pinned by a unit test. Habit at 10 MB scale = muscle memory at 10 TB scale.

**Dry run (BigQuery)** — `QueryJobConfig(dry_run=True)`: the server validates SQL syntax and parameter types and returns `total_bytes_processed` **without executing or billing** — the `terraform plan` of query jobs. Catches the bug classes that live inside SQL strings where `py_compile` and string-assertion tests can't see (clause-order errors, typos, param-type mismatches).

**Billing-DoS** — An open, unauthenticated endpoint over BigQuery where each request runs a **paid query job** — both data exposure and a cost-exhaustion attack. Mitigated by auth + rate limiting + `maximum_bytes_billed` before any public deploy.

---

## Reference values (this project)

| Thing | Value |
|---|---|
| GCP project | `devpulse-dp2622` |
| Region | `us-central1` (regional) |
| Bronze bucket | `devpulse-dp2622-bronze` |
| Silver bucket | `devpulse-dp2622-silver` (Parquet at `events/`) |
| Silver dataset / table | `devpulse_silver` / `silver_events` (HOUR-partitioned on `created_at`) |
| Gold dataset | `devpulse_gold` (dbt target) |
| Pipeline SA | `devpulse-pipeline@devpulse-dp2622.iam.gserviceaccount.com` |
| Canonical test hour | `2024-01-01 15:00` — **180,386** rows (180,387 raw, 1 dupe) |
| Run metadata table | `devpulse_silver.pipeline_run_metadata` — unpartitioned (KB-scale), free load-job writes, free `list_rows` reads |
| Source URL pattern | `https://data.gharchive.org/YYYY-MM-DD-H.json.gz` |
