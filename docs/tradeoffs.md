# Technology tradeoffs — the honest steelmen

> `decisions.md` records what each decision rejected and why we were right. This doc argues the **other side
> properly**: what every major technology choice *buys*, what it *costs*, and — the part interviews test —
> **when the alternative wins**. Use it as `/quiz` material: a tradeoff answer that can't argue both
> directions is half an answer (the Day 13 quiz proved it).
> Format per entry: Chose → It buys → It costs → When the alternative wins → the steelman said aloud.

## BigQuery (warehouse) — vs Snowflake, Redshift, self-hosted Postgres

**Buys:** zero-ops serverless (no clusters, no vacuuming), a free tier that makes this project possible, free
native load jobs (the whole silver→BQ hop costs nothing), HOUR partitioning + the `$decorator` that carries our
idempotency, and heavy job-market demand.
**Costs:** the idempotency mechanism is **vendor-specific** (partition decorators don't exist elsewhere — porting
means redesigning the load); bytes-scanned pricing punishes careless SQL (our quota incident); no per-query cost
cap unless you set one; console-only knobs (billing alerts) escape Terraform.
**When the alternative wins:** Postgres wins when data fits one node and the team already runs it — no new cost
model, real indexes, no per-query anxiety. Snowflake wins on multi-cloud and finer compute isolation
(per-workload warehouses). **Steelman aloud:** *"At my data size, a single Postgres box would be simpler, cheaper
to reason about, and portable — BigQuery earns its place only because the project is deliberately built on
warehouse-scale patterns."*

## GCS medallion lake (bronze/silver) — vs ELT straight into the warehouse

**Buys:** bronze as a replayable source of truth (every reprocess, backfill, and red-path drill this project ran
depended on it), cheap storage decoupled from compute, Parquet as a durable open-format contract (BQ is a
swappable load target), and a place for the quarantine to exist at all.
**Costs:** two copies of everything; a second storage system to secure, lifecycle, and reconcile; the
bronze→silver→BQ seams are exactly where our counting/reconciliation machinery had to be built — ELT has fewer
seams to guard.
**When the alternative wins:** load-raw-to-warehouse + dbt-from-raw wins for small teams with pure-SQL
transforms — one system, one bill, dbt tests see *everything* because everything loads (Day 13's whole gate
exists because our bad rows never load). **Steelman aloud:** *"ELT would collapse three storage layers into one
and make my quality story simpler — the lake pays off only when reprocessing, open formats, or non-SQL transforms
matter."*

## PySpark, single-node (silver transform) — vs pandas/DuckDB, vs BQ SQL

**Buys:** the distributed *API as a contract* — explicit schemas, partitioned writes, dynamic partition
overwrite; the same job scales to a cluster (Dataproc/EMR) with config, not rewrites; the skill the job market
actually lists.
**Costs:** absurd overhead at our size — a JVM, a 4g driver, a custom image with connector jars, an OOM incident,
~1 min per hour of data for work **DuckDB or pandas would do in seconds in-process**. Single-node Spark has all
of Spark's operational weight and none of its parallelism payoff.
**When the alternative wins:** at this project's actual scale, *the alternative simply wins* — pandas/DuckDB
inside the ingestion process would be faster, simpler, and dependency-light. Spark is here to learn the at-scale
tool honestly. **Steelman aloud:** *"For one 180k-row hour, Spark is a truck delivering a letter — I chose it to
practice driving the truck, and I can say precisely what changes at 100× (cluster, Dataproc, the same code)."*

## Airflow — vs cron + scripts, vs Dagster/Prefect

**Buys:** the industry-standard orchestration vocabulary (sensors, retries, backfills, trigger rules, data
intervals — each one a lesson this project needed), a real scheduler with state, per-task observability that made
every red-path drill legible, and DockerOperator as the per-tool-image glue.
**Costs:** heavyweight locally (webserver+scheduler+triggerer for one DAG); semantic footguns that bit us twice
(interval-end-vs-start on manual triggers; the paused flag living in the metadata DB, not code); config-as-Python
sprawl; testing DAG logic is awkward.
**When the alternative wins:** cron + a shell script wins for one linear pipeline — everything we built through
Day 4 needed nothing more. Dagster wins on developer ergonomics: asset-oriented (its native concepts map to our
tables, not our tasks), first-class local testing, typed IO. **Steelman aloud:** *"For a single hourly chain,
cron would do; Airflow earns its complexity through backfills, retries, and gates — and if I started today I'd
seriously evaluate Dagster, whose asset model matches how data teams actually think."*

## dbt (gold layer) — vs hand-rolled SQL scripts, vs Spark for transforms

**Buys:** the `ref()` DAG (dependency order + lineage + docs generated from code), tests as first-class citizens
(our 69-test gate), incremental materializations with merge semantics, and the de-facto standard for
warehouse transformation hiring.
**Costs:** a Jinja indirection layer between you and the SQL; its own dependency island (it broke our `.venv` —
the Day 8 scar that spawned the per-tool-image rule); **its tests only see loaded data** (the blind spot that
justified Day 13); state (`--full-refresh` after the merge-can't-un-merge incident) requires understanding dbt's
model of the world, not just SQL.
**When the alternative wins:** plain SQL scripts win for ≤5 tables — the DAG solves an ordering problem you don't
have. Spark wins when transforms stop being relational (ML features, nested payload work). **Steelman aloud:**
*"dbt is SQL with a build system — below a handful of models the build system is overhead; my 9 models with 4
enforced FK relationships is roughly where it starts paying."*

## Great Expectations — vs dbt tests alone, vs pandera, vs 30 lines of asserts

**Buys:** jurisdiction dbt can't have (validates *before* load — the quarantine is invisible to warehouse tests);
suites as committed, reviewable JSON contracts; a machine-readable Validation Result that alerting/metadata
consume (Day 14); engine portability (the same suite runs pandas → Spark → BQ as data grows).
**Costs:** a *heavy* framework for what are conceptually simple checks — context/datasource/asset/batch/suite/
validation-definition registries before one expectation runs; 1.x API churn (dead 0.x examples everywhere;
`add_or_update` append semantics surprised us); its own image + pins. Our counted quarantine checks ended up as
**plain Python anyway** — GE's registry added ceremony, not power, there.
**When the alternative wins:** **pandera** wins for in-process DataFrame validation — declarative, pytest-native,
a tenth the weight; hand-rolled asserts win when checks are few and consumers are only humans; dbt tests alone win
when everything loads (no quarantine to count). **Steelman aloud:** *"GE's real product is the standardized
artifact and engine portability — if I only needed schema-and-null checks on one pandas frame, pandera would do
it in 20 lines, and honestly half my Day 13 value came from plain-Python counting that GE couldn't express."*

## Terraform — vs console + gcloud scripts, vs Pulumi

**Buys:** the daily destroy/apply rhythm this whole project's cost hygiene depends on (9 resources, one command,
reproducible); infra changes as reviewable diffs; state as the record of what exists.
**Costs:** state is a thing to manage and a thing to corrupt; drift the moment anything is done in the console
(our billing alerts + byte quota live outside it, permanently); HCL is another language with its own gotchas.
**When the alternative wins:** a 20-line `gcloud` script wins for a fixed personal-project footprint — arguably
including ours; Pulumi wins when your team wants real-language control flow and testing for infra. **Steelman
aloud:** *"Nine resources don't need Terraform — a script would do; Terraform earns it the day infra is shared,
reviewed, or environment-multiplied, and I wanted the workflow that survives that day."*

## Per-tool Docker images — vs one project venv/image

**Buys:** dependency isolation with receipts (dbt broke the shared `.venv` on Day 8 — the founding scar); each
tool pinned and upgraded independently; dev-loop containers and Airflow DockerOperators run the *same* image
(one dbt everywhere); the shape CI and K8s expect later.
**Costs:** five stacks to build, version, and rebuild; the compose-vs-operator contract must be hand-mirrored
(the entrypoint trap bit Day 12 *and* Day 13's env dicts); DockerOperator rides the host socket (a root-equivalent
surface we accept for local dev); gigabytes of images for kilobytes of code.
**When the alternative wins:** one well-pinned venv wins for a solo, short-lived project with compatible deps —
it's what every step before Day 8 used happily. **Steelman aloud:** *"Isolation is insurance — I bought it after
paying the deductible once; a solo project with stable deps could reasonably self-insure."*

## FastAPI directly over BigQuery (serving) — vs a serving DB (Postgres/Redis), vs a BI tool

**Buys:** zero new infrastructure — the warehouse the pipeline already fills is the only store, and the dbt
marts stay the single source of computed truth ("the warehouse computes, the API serves": endpoints do filter +
order + paginate, never aggregate); the whole cost surface is a parameterized, byte-capped query layer plus a
free `list_rows` ops path; a TTL cache absorbs repeat traffic for pennies.
**Costs:** every mart request is a **billed query job** with warehouse latency (seconds, and a 10 MB billing
floor) — BigQuery is a scan engine, not a point-lookup store (no indexes, no connection pooling); the in-process
cache is per-worker (two uvicorn workers = two caches); auth + rate limiting are still owed before any deploy —
an open endpoint over BQ is a **billing-DoS** surface.
**When the alternative wins:** a serving DB wins the moment traffic is real — user-facing p95s, high QPS, deep
pagination (keyset over an index), per-request cost that must round to zero. The shape is reverse ETL: dbt
publishes the marts *into* Postgres (or hot keys into Redis) and BigQuery goes back to being the compute tier —
and the API's existing seams (pure builders + injected client) are exactly where that swap lands without
rewriting endpoints. A BI tool (Metabase/Looker) wins when the consumers are analysts, not applications.
**Steelman aloud:** *"At localhost scale the warehouse doubles as the serving layer for pennies; the day this
fronts real traffic, the marts get published into a serving DB and the code seam for that swap already exists."*
