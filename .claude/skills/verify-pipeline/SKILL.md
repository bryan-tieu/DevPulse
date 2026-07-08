---
name: verify-pipeline
description: Prove idempotency and reconcile row counts for any DevPulse layer (bronze, silver Parquet, silver BQ, dbt gold). Use after building or changing any pipeline step, before documenting it as done, or when the user asks "did this actually work / is it idempotent".
---

# Verify-pipeline: prove it, don't claim it

Two proofs, always both: **(1) idempotency** — run it twice, the second run must change nothing; **(2) reconciliation** — counts match the layer upstream. A green run without these is unverified.

Canonical hour `2024-01-01 15:00`: raw **180,387** → deduped **180,386**. All other canonical counts in `CLAUDE.md` → Reference values.

## Per-layer procedures

**Bronze (ingestion):** re-run `ingest_hour('2024-01-01', 15)` → must print "Already in bronze, skipping" (the `blob.exists()` guard). One object at `date=2024-01-01/hour=15/`.

**Silver Parquet (Spark):** re-run the `spark-submit` → dynamic partition overwrite replaces the hour in place. Then list the partition:
```
python -c "from google.cloud import storage; b=storage.Client().bucket('devpulse-dp2622-silver'); fs=[x.name for x in b.list_blobs(prefix='events/event_date=2024-01-01/event_hour=15/')]; print(len(fs))"
```
Expect **12** files, **0** orphaned `.spark-staging-*` dirs (a leftover staging dir = a failed commit to clean up).

**Silver in BigQuery:** re-run `python -m transform.load_silver` (decorator + WRITE_TRUNCATE replaces the partition), then:
```
python -c "from google.cloud import bigquery; print(list(bigquery.Client(project='devpulse-dp2622').query('SELECT COUNT(*) c FROM devpulse_silver.silver_events').result())[0].c)"
```
Steady at **180,386** — not doubled (doubling means an append slipped in).

**dbt gold:** second `dbt build` (or `dbt run -s fact_events`) → the fact flips `CREATE TABLE`→`MERGE`, row count steady at 180,386, 0 rows inserted. All tests green (PASS count = baseline). Reconciliations:
- `stg_events` count = silver count = 180,386
- `trending_repos_daily`: `SUM(stars)` = fact `COUNT(*)` where `event_type='WatchEvent'`
- `language_momentum`: `SUM(event_count)` = **180,386** (LEFT join dropped nothing); `SUM` of distinct-repo measure ties to `dim_repo` grain (55,245)
- `contributor_leaderboard`: `SUM(contributions)` = **163,953** (fact filtered to the contribution allowlist)

**Airflow (full chain):** clear + re-run the interval (or `dags test`) → GCS file count and BQ row count unchanged. Remember: a manual `@hourly` trigger's data interval is the window *ending at* the logical date (process 15:00 by triggering 16:00).

## Interpreting failures

- **Count doubled** → an append where a replace belongs (check write disposition / overwrite mode / partition decorator).
- **Count dropped** → a silent filter or an INNER join against partial data — find the dropped rows before "fixing" the number.
- **New-hour work:** these exact numbers only hold for the canonical hour. For any other hour, establish the raw count first (Spark `count()` on the bronze read), then hold every layer to it.
- Report results with the numbers, pass/fail per layer, and one line on what each proof demonstrates (interview framing).
