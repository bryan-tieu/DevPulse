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
