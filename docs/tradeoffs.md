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

## dbt incremental `merge` (the fact) — vs `insert_overwrite`, vs full-refresh every build

**Buys:** correctness keyed to *identity*, not discipline — an upsert on `unique_key='event_id'` cannot duplicate
no matter what window gets reprocessed (late-arriving data, overlapping backfills, a nervous re-run); the grain
guard and `on_schema_change='fail'` ride along.
**Costs:** MERGE scans the *target* to find matches — measured honestly: the second build scanned **more** than
the full rebuild (**8.5 → 40.4 MiB**; one partition + full-hour lookback = nothing to prune); and a merge
**cannot subtract** — rows in the target but absent from the source are invisible to it, so bad-data cleanup
needs `--full-refresh` (Day 12, learned live).
**When the alternative wins:** `insert_overwrite` wins for immutable event data whose loads always align to
partition boundaries — replace partitions wholesale, never scan the target; arguably the textbook fit for *this*
data. Full-refresh-every-build wins while the table is small enough that rebuild ≈ merge cost (ours is — measured).
**Steelman aloud:** *"insert_overwrite is probably the textbook choice for immutable events; I chose merge to make
correctness unconditional on my partition discipline, measured that it scans more at my scale, and banked exactly
when that tradeoff flips."*

## Committed enrichment seed (`repo_languages`) — vs a live GitHub-API metadata source

**Buys:** zero credentials, rate limits, or network in the build; deterministic and reviewable (the seed is a
diffable CSV); the mart reads it via `ref()`, so swapping in a real source later never touches the SQL.
**Costs:** hand-authored → *cannot* be accurate — the banked finding: a single-hour firehose is dominated by
opaque bot/automation repos no human can label by inspection (4 matched repos; 178,081 events `Unknown`); static
while reality drifts; needed `column_types` pinning to survive the join.
**When the alternative wins:** the real repos-API source wins the moment coverage or freshness matters — that's
its own small pipeline (ingestion, refresh cadence, rate-limit budget), which is exactly the cost the seed defers.
**Steelman aloud:** *"The seed is an honest stand-in: it proved the LEFT-join + COALESCE shape with 4 repos, and
its failure to cover bot-dominated data is itself the measured argument for real API enrichment at scale."*

## Free BigQuery paths everywhere (load jobs in, `list_rows` out) — vs streaming inserts, vs query jobs

**Buys:** the pipeline's entire write path (Parquet loads, `load_table_from_json` run metadata) and its ops read
path (`/runs` via `tabledata.list`) bill **zero** — proven from job history; batch load semantics match the
hourly grain exactly.
**Costs:** load jobs are batch-latency (seconds-to-visible, fine here); `tabledata` reads have no `ORDER BY`/
filter — read-all-then-sort whose *physical* costs (transfer, memory, sort) grow with the table even though the
bill stays zero.
**When the alternative wins:** streaming inserts win when sub-second availability is the product (Phase 4's
stretch shape) — you pay per-MB and inherit dedupe complexity. A query job wins the moment you need "the newest
10 of a million" — and that switch flips `/runs` into a billed endpoint that then *earns* the TTL cache it
deliberately lacks today.
**Steelman aloud:** *"Cost-aware engineering here meant knowing which BigQuery* API *each byte rides, not just
what the data is; every path is free because the grain is hourly batch, and I know which knob starts billing the
day the grain changes."*

## Webhook alerting (`on_failure_callback` → POST) — vs email/SMTP, vs PagerDuty-class tooling

**Buys:** one pure function + `requests.post` with a timeout (an alert path that can hang its patient is a bug);
lands where the human already looks; composes with retry routing so it pages once, after retries exhaust, same
minute as a gate failure (measured: 11 min → seconds).
**Costs:** no escalation, acknowledgment, or dedup windows; the URL is a secret currently living in `.env`
(secrets backend deferred to Phase 4); delivery is best-effort — a failed POST is logged, not retried.
**When the alternative wins:** PagerDuty/Opsgenie win the moment more than one human is on call — escalation
policies and ack tracking are the product; email wins never (it's where alerts go to be ignored).
**Steelman aloud:** *"A webhook is the smallest alert that actually interrupts a human; the day this has an
on-call rotation, the callback body stays and the destination becomes PagerDuty — the routing logic is the part
I built, and it transfers."*

## Streamlit dashboard **through the API** — vs Streamlit straight to BigQuery, vs Looker Studio, vs a React SPA

**Buys:** one guarded door to the warehouse — bound parameters, `maximum_bytes_billed`, deterministic `ORDER BY`
and the TTL cache all stay in `api/`, and the dashboard structurally cannot bypass them (enforced: no `bigquery`
import, no SQL string under `dashboard/`); the dashboard holds **no credentials at all**; Python end-to-end with
no JS toolchain; an HTTP client with a real test seam (fake transport, zero live API).
**Costs:** an extra hop and a serialization round-trip; Streamlit re-executes the whole script on every widget
interaction, so caching is load-bearing architecture rather than polish — and it **stacks** with the API's TTL,
making worst-case staleness the *sum* of the two while a refresh button clears only the near layer; single
process, single user, no auth, no horizontal story.
**When the alternative wins:** *Streamlit → BQ directly* wins when the dashboard is the only consumer and no API
exists — one hop, nothing to keep in sync (it loses the moment a second consumer appears). *Looker Studio / any
BI tool* wins for business users needing self-serve exploration and scheduled delivery with **no code to
maintain** — the right answer at a company, and the wrong one here precisely because it would connect straight to
BQ and skip the API, leaving nothing engineered to defend. *A React SPA* wins for real product UX, mobile, or
public traffic — at the price of a JS stack that teaches nothing about data engineering.
**Steelman aloud:** *"The dashboard is a consumer of my API, not a second client of my warehouse — everything
that makes warehouse access safe sits behind one door, and a second `bigquery.Client` would be an uncapped,
unparameterized path plus a copy of my SQL that drifts. Streamlit is honest for one developer on localhost; the
at-scale shape is a real frontend against an authenticated, rate-limited API, and that seam already exists."*
