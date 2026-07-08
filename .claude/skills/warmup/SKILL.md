---
name: warmup
description: Run a from-scratch re-implementation drill of an existing DevPulse component — Bryan rebuilds it cold from a blank file, Claude verifies and grades. Use when the user says "warm-up", asks to practice coding, or at the start of a session before new build work.
---

# Warmup: rebuild it cold

Days 1–11 were ~90% Claude-written. Bryan knows the code *conceptually*, not *cold* — so warm-ups treat him as starting from Day 1 strictly coding-wise. He rebuilds real components from a blank file; the originals are the answer key, not the starting point.

## The ladder (dependency order — work top to bottom; `docs/warmups.md` tracks status)

1. `bronze_key()` + `ingest_hour()` — idempotent GCS ingestion (orig: `ingestion/ingest.py`)
2. `_hour_partition()` + `load_silver()` — BQ Parquet load job, explicit schema, `$YYYYMMDDHH` decorator (orig: `transform/load_silver.py`; **verify: `tests/test_load_silver.py`**)
3. Spark `SCHEMA` + `transform_events()` — flatten, dedupe, timestamp cast (orig: `spark/silver_events.py`; **verify: `tests/test_silver_events.py`, in-container**)
4. Spark `run()` + session config — dynamic partition overwrite, GCS auth, why driver memory can't be set in-code
5. The Airflow DAG — sensor (reschedule), interval-derived date/hour, task chain, retries (orig: `airflow/dags/devpulse_ingest.py`)
6. `stg_events.sql` + one surrogate-key dim with `QUALIFY` latest-wins (orig: `dbt/models/staging/`, `marts/dim_repo.sql`)
7. `dim_date` spine + the incremental `fact_events` — config block, deterministic FKs, watermark + lookback
8. One mart + its `.yml` — window functions, LEFT-join enrichment, grain guard (orig: `dbt/models/marts/`)
9. Terraform core — bucket, dataset, SA, additive IAM grants (orig: `terraform/main.tf`)

## Protocol (per drill, ~20–40 min timeboxed)

1. **Pick the target:** next 🔁/⬜ item in `docs/warmups.md`, or the user's choice. Re-test any ✅ item older than ~a week before advancing past it.
2. **Concept check first (2 min):** Bryan states in 2–3 sentences what the component does and the one design rule it embodies (e.g. "partition grain = load grain"). Gaps here → quick `/teach` refresher *before* coding, not during.
3. **Blank file, no peeking.** He writes in `warmups/<item>-<date>.py|.sql` (gitignored). The original file must stay closed — the coach-mode hint ladder applies (concept → shape → snippet → full solution, escalate only on request; level 3+ automatically caps the grade at 🔶).
4. **Verify:** run the existing pytest where one exists (items 2–3); otherwise dry-run what's cheap (dbt compile/parse, `terraform validate`, DAG import) — no cloud infra needed for most drills; don't apply/spend to verify a warm-up.
5. **Diff review together:** compare against the original line by line. Every difference is either (a) a real miss — name the concept it maps to, or (b) a legitimate alternative — say so, don't fake-fail him. Style deltas don't count against the grade.
6. **Grade and log** in `docs/warmups.md`: ✅ cold (works, structurally right, hints ≤ level 1) · 🔶 warm (needed shape hints or had a real gap — schedule a repeat) · 🔁 repeat (didn't get there — normal, log what blocked). One line of notes: what was missed, what to focus on next rep.

## Rules

- **Never overwrite real project code** — warm-ups live in `warmups/` only.
- Don't do two new ladder items in one sitting; one drill, done honestly, beats three rushed.
- The point is production under pressure: keep the timebox, let him struggle before hinting (struggle *is* the rep), and be straight in grading — an inflated ✅ just resurfaces in an interview.
- An item is only retired after passing cold **twice**, at least a week apart.
