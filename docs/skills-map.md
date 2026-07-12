# Skills Map — what DevPulse proves, and where

> The bridge from "I built a project" to "hire me": every data-engineer job-description skill, mapped to the concrete evidence in this repo and the one-line interview claim it supports.
> Status: ✅ proven (built + verified) · 🔶 exercised (pattern built, not stress-tested at scale — say so honestly) · ⬜ planned (phase noted).
> `/end-session` updates this file when a day flips a status.

| Skill (as job posts phrase it) | Status | Evidence in this repo | Interview claim |
|---|---|---|---|
| Cloud data platforms (GCP: GCS, BigQuery) | ✅ | Terraform-provisioned buckets/datasets; free-tier + regional co-location cost design ([decisions Day 2](decisions.md)) | "I designed for bytes-scanned billing: partition pruning, free load jobs over streaming inserts, regional co-location." |
| Lakehouse / medallion architecture | ✅ | Bronze (immutable GCS) → silver (Spark Parquet → BQ) → gold (dbt star) end to end | "I can explain why each layer exists and what contract it exposes to the next." |
| Orchestration (Airflow) | ✅ | `airflow/dags/devpulse_ingest.py` (7 tasks): sensor → ingest → Spark → **GE gate** → load → **dbt gate** → `all_done` observer; retry routing + failure callbacks, reschedule-mode sensor, bounded 48h backfill, pinned-interval triggers (and the off-by-one war story) | "My tasks are pure functions of the data interval — that's what makes them retryable and backfillable." |
| Distributed processing (Spark/PySpark) | 🔶 | `spark/silver_events.py`: explicit schema, flatten, dedupe, dynamic partition overwrite; single-node by design | "Single-node was deliberate; I can articulate the cluster deltas — executor sizing, shuffle cost, Dataproc/EMR submission." |
| Warehouse modeling (Kimball star schema) | ✅ | 4 dims (surrogate keys, Type-1 SCD, generated date spine) + factless incremental `fact_events` + 3 marts | "I chose merge over insert_overwrite knowingly, and I measured that MERGE scanned *more* at small scale — pruning pays only at volume." |
| dbt (models, tests, docs, seeds, packages) | ✅ | `dbt/`: sources→staging→marts, 69-test build, lineage docs, `dbt_utils`, enrichment seed | "Every model ships with grain tests and relationships tests; the build fails loudly on bad data." |
| Idempotent pipeline design | ✅ | `blob.exists()` skip · dynamic partition overwrite · `table$YYYYMMDDHH` + WRITE_TRUNCATE · dbt merge — each proven by re-run | "Every layer re-runs to the same counts: 180,386, twice." |
| Data quality gates | ✅ | Two gates, both proven green *and* red: `dbt_build` (Day 12 — first live catch was real bad data) + GE `validate_silver` with the counted identity 180,387 = 180,386 + 0 + 1 (Day 13); red path re-proven under alerting (Day 14) | "Quality checks are gates that fail the run, not dashboards nobody reads — mine caught real bad data on its first live run, and a poisoned row stops the pipeline before BigQuery." |
| IaC (Terraform) | ✅ | `terraform/`: buckets, datasets, least-privilege SA/IAM; daily apply/destroy cycle; lockfile committed | "Additive IAM members over authoritative bindings — smallest blast radius." |
| Security & credentials | ✅/🔶 | Keyless ADC everywhere, no SA key ever minted, least-privilege IAM, PII (author emails) excluded from silver/gold | "Short-lived credentials only; the pipeline SA switch is impersonation, not a key." (Pending: the actual SA switch.) |
| Docker / containerized tooling | ✅ | Airflow, Spark, dbt each in isolated images (dbt isolated *because* its deps broke the shared venv); DockerOperator/DooD | "Per-tool isolation isn't cosmetic — I hit the dependency conflict that justifies it." |
| Testing (pytest) | 🔶 | Pure-transform/I-O split → unit tests for Spark transform + load-path derivation | "I structure code so logic is testable without a cluster." |
| Cost engineering | ✅ | Free load jobs, partition pruning, 10 MB query floor understood, daily teardown, budget alerts; `maximum_bytes_billed` planned | "This platform costs approximately $0 because every access path was chosen with the billing model in mind." |
| Serving (FastAPI) & dashboards | ⬜ | Phase 3 — API over gold marts (pagination, caching, parameterized queries, auth, rate limits) + dashboard | — |
| CI/CD (GitHub Actions) | ⬜ | Phase 3 — lint/pytest/dbt build/GE on PR; image builds; pinned deps + Dependabot | — |
| Streaming (Kafka + Structured Streaming) | ⬜ | Phase 4 stretch — Events API → Kafka → micro-batches | — |
| Observability / run metadata & alerting | ✅ | Day 14: webhook alert via `on_failure_callback` + retry routing by failure class (`retries=0` on gates — 11 min → same-minute page, measured); `pipeline_run_metadata` written by an `all_done` observer via free load jobs; both paths proven (one page on red, row lands anyway) | "Retry policy is a routing decision: infra retries silently because idempotency makes re-runs free; data gates page immediately because deterministic failures don't heal. I measured the difference: 11 minutes to seconds." |

## How to use this in the job hunt

- **Resume bullets** come from the *Interview claim* column — each is a claim + a number + a mechanism.
- **The 🔶 rows are strengths, not weaknesses**: "exercised, not stress-tested, and here's exactly what changes at scale" is a more senior answer than pretending a laptop project is production. Practice those deltas with `/quiz`.
- Keep the honest-negative findings ready (MERGE scanned more; bot-dominated firehose broke the hand-seed): war stories about measurement beat success stories about tutorials.
