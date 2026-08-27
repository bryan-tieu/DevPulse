# 07 · Behavioral & at-scale

The questions that decide the interview. Technical depth gets you past the screen; these get you the offer.

---

## Topic: The pitch

### Q: Tell me about this project in 60 seconds.

`Coached 2026-07-30 · re-tested 2026-07-31 — the inverted framing held`

**The framing correction that mattered.** The original version led with *"I built it to teach myself data engineering"* and closed on *"these are stepping stones, not the bridge."* Both instincts are backwards: opening with "to teach myself" tells the interviewer to discount everything that follows, and closing on the concession makes the *limitation* your final impression.

**Invert it:** lead with the **concrete build and its scale**, put the **at-scale judgment** in the middle as the differentiator, and **close on confidence**. Keep the honest limitation in your pocket for the "what's simplified?" follow-up — where it reads as self-awareness rather than apology.

**Model answer:**
> "DevPulse ingests GitHub's global public event stream — every push, star, PR and issue across all of GitHub, hourly — through a medallion lakehouse on GCP. Raw gzipped JSON lands immutably in GCS, PySpark flattens and dedupes it to Parquet and into a partitioned BigQuery table, and dbt models it into a Kimball star schema with four dimensions, an incremental factless fact, and three serving marts. Airflow orchestrates it as seven tasks with two quality gates — Great Expectations between bronze and silver, dbt tests before gold — and both gates fail the run rather than reporting to a dashboard. Terraform provisions everything, and a FastAPI service plus a Streamlit dashboard serve it. The part I'd want to talk about is the engineering judgment: every layer is idempotent by a *specific* mechanism, I measured that MERGE scanned 4.75× more than the initial build and can tell you exactly why, and I can name what changes when this is a hundred times bigger."

**Follow-ups to expect:** every question in files 01–06.

**Drill:** time it. If it runs past 90 seconds, cut the tool list, never the numbers.

---

### Q: What's simplified about it? What would you do differently?

`❓ Predicted probe — not yet drilled` *(this is where the honest concessions belong)*

**Model answer:**
> "Several things, deliberately. **Spark is single-node in Docker** — at 20 MB an hour a cluster would be theatre, and I'd rather say that than pretend; what changes at real volume is executor sizing, shuffle partition tuning toward 128–200 MB apiece, and submission through Dataproc. **Enrichment is a seed, not an API** — which is why 98.7% of events land in an `Unknown` language bucket; the real version is a GraphQL batch fetch behind an incremental anti-join. **`dim_date` is a static 2024 spine**, which is correct for this dataset and a real defect class in general, because a hand-frozen calendar silently ages out. **Airflow is local**, the API and dashboard are localhost-only with no auth or rate limiting, and I've deliberately *not* deployed because an unauthenticated API over BigQuery is a billing-DoS. What I'd do differently: I'd have built the CI gate earlier — I've been the only thing standing between a broken commit and `main` for 128 commits."

**Follow-ups to expect:**
- *"Why is `Unknown` at 98.7% acceptable?"* → because the alternative — an INNER join — makes it *invisible* rather than *smaller*. Coverage is the problem; hiding it isn't the fix.
- *"What's your biggest unresolved question?"* → whether `PERMISSIVE` mode with an undeclared `_corrupt_record` column is silently dropping unparseable lines. Logged as an open experiment, not assumed either way.

---

## Topic: At-scale reasoning

### Q: This runs on one laptop. What breaks at 100× — and at 10,000×?

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "At 100× — roughly 2 GB an hour — honestly, **not much breaks.** A single beefy node still handles that, and saying so is the point; the first thing I'd tune isn't infrastructure, it's `spark.sql.shuffle.partitions`, because the default 200 is wrong in both directions. At 10,000× the shape changes: Spark needs a real cluster with executor sizing and skew handling, though for my dedupe specifically the better move is to exploit the existing hour partitioning and make the wide transformation narrow rather than tuning the shuffle. BigQuery-side, the MERGE strategy finally earns its keep — an incremental run touching one partition out of months prunes to a sliver, which is exactly the payoff I measured myself *not* getting over one hour. Bronze storage becomes a real line item rather than $42 a year, so lifecycle policies stop being optional. And Airflow's scheduler and pool sizing become the actual constraint long before the data does — which is why my sensor is in reschedule mode."

**Follow-ups to expect:**
- *"What's your bottleneck right now?"* → nothing technical. It's a single-node laptop project by choice, and the honest constraint is that patterns are **exercised, not stress-tested**.
- *"How would you know when to move?"* → measure. The MERGE finding is the template: I had a documented expectation, measured against it, and got the opposite result — so I logged the negative result rather than the assumption.

---

### Q: Tell me about something that didn't work.

`❓ Predicted probe — not yet drilled · you have unusually good material here — pick ONE and tell it fully`

**Three candidates, ranked by how well they land:**

**1. The silent delete (best — it's about *tooling* lying to you).**
> "During a cleanup I deleted a stray BigQuery partition and it reported success. It had removed nothing — I'd aimed at a misread hour, and `bq rm -f -t` on a *nonexistent* partition succeeds silently. What caught it was checking `INFORMATION_SCHEMA.PARTITIONS`, a free metadata query, instead of trusting the exit code. Then the second edge: the gold rows the incremental fact had already merged were unreachable by a partition delete — a merge can't un-merge — so restoration meant `dbt build --full-refresh`, verified back to PASS=69 and 180,386. The rule I took away was partition grain = load grain = **delete grain**, and the habit was artifact-first: make the system tell you what happened rather than believing the command."

**2. The MERGE cost surprise (best for showing measurement).** 8.5 MiB → 40.4 MiB. See [04-modeling-and-dbt.md](04-modeling-and-dbt.md).

**3. The under-summing reconciliation.** A mart summed to 710 stars instead of 7,236; diagnosed alone from the uvicorn access log — every request showed `offset=0`, so the pagination parameters were never leaving the client. The lesson: the access log is the ground truth of what crossed the wire.

**Follow-ups to expect:** *"How did you find it?"* — always answer with the **artifact** you consulted, not the reasoning you did. `INFORMATION_SCHEMA.PARTITIONS`, the access log, `bq ls -j`, `bq show --schema`. Artifact-first debugging is a demonstrable habit and it's yours.

---

## Delivery reminders

Three sentences, if you keep nothing else from this folder:

1. **Lead with the number.** You have them; they're what makes this not a tutorial.
2. **Name the failure, not just the mechanism.** *"It succeeds and lies"* beats *"it doesn't work."*
3. **Never claim a benefit you can't point at.** The honest concession is already in your docs, and it's stronger than the defense.
