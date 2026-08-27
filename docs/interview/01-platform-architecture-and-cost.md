# 01 · Platform, architecture & cost

Skills-map rows: **Cloud data platforms (GCS, BigQuery)** · **Lakehouse / medallion architecture** · **Cost engineering**

---

## Topic: Cloud data platforms (GCS, BigQuery)

### Q: Your claim is "I designed for bytes-scanned billing." Name the three mechanisms, and for each, what the naive alternative would have cost you.

`Asked 2026-08-06 · skills-map quiz Q1 · 7/10`

**My answer:**
> Partition pruning locates only the rows we are interested in looking at. A simple WHERE clause to determine which date to look at so we dont scan entire tables just to look at a couple rows. The naive alternative costs real money and bytes scanned, which is GCS primary billing method. Load jobs over streaming inserts are free. Once we introduce streaming inserts we pay a fee for ingestion. Taking advantage of free load jobs allows this project to stay under budget. Regional co location reduces latency if we were to have compute and storage in separate locations. Keeping them regional keeps latency low because we don't have to worry about data travelling across the network from one region to another.

**Verdict:** All three mechanisms named unprompted — good. Two problems: **"bytes scanned, which is GCS primary billing method" is wrong** (that's BigQuery; GCS bills storage × GB-month, operations, and egress), and **zero numbers** despite being asked for them. Regional co-location was framed as latency only — it is actually a **hard constraint**.

**Why the GCS/BigQuery slip costs more than one point:** it's the most basic distinction in GCP data engineering. An interviewer who hears a foundational slip stops probing depth and starts checking fundamentals — you spend the rest of the interview proving things you already knew instead of showing the merge measurement.

**Model answer:**
> "BigQuery bills on bytes scanned with a 10 MB floor per query, so I designed three things around it. `silver_events` is HOUR-partitioned on `created_at`, so an hour-scoped query touches one partition rather than the table — and because storage is columnar, I select only needed columns; scanning is partitions × columns, never rows. I load with load jobs rather than streaming inserts: free, and critically they support the `$YYYYMMDDHH` partition decorator with WRITE_TRUNCATE, which is *how* my re-runs stay idempotent. Bucket and dataset are both `us-central1` because a load job requires compatible locations — cross-region isn't slow, it's disallowed, plus egress charges. GCS itself bills separately on storage and operations; the bytes-scanned model is BigQuery's."

**Follow-ups to expect:**
- *"What does GCS actually bill for, then?"* → storage class × GB-month, Class A/B operations, egress, early-deletion charges on Nearline/Coldline.
- *"Same WHERE clause, `SELECT *` vs `SELECT actor_login` — same bytes?"* → **Answered correctly on the day.** No: bytes scanned is partitions × columns. Column pruning is a *separate* mechanism from partition pruning.
- *"What's the smallest possible query bill?"* → 10 MB minimum, which is why 50 tiny queries cost more than one scan of the hour.

**Drill:** say aloud five times — *"GCS bills storage and operations. BigQuery bills bytes scanned."* Rote error, rote fix.

---

## Topic: Lakehouse / medallion architecture

### Q: GH Archive is already a durable public archive at a stable URL. Defend your bronze layer — then name the conditions under which you'd drop it.

`Asked 2026-08-06 · skills-map quiz Q2 · 7/10`

**My answer:**
> Bronze remains the single source of immutable truth in GCS. If we ever did a rerun on the source URL but didn't know the data within it changed, it breaks my pipeline. Having bronze be the single source of truth instead of the URL means I remain in control of the data that flows through the pipeline. If I knew the source URL never changes its payload, loading straight to silver would be more optimal, removing the paid storage.

> *(follow-up — size the cost)* It's about $3.5 a year to store all 8760 hourly files which is minimal. We start seeing gains from not having to manage another system in our pipeline. Reconciling the seams between bronze and silver to ensure data isn't missing and lands in silver properly. dbt sees all the data since we're loading straight into the warehouse

**Verdict:** Reproducibility-and-control is the right core, and "one less seam to reconcile" is a mature steelman. Three problems: **$3.50 is the *monthly* figure** (~$42/year steady state — a 12× units error, and volunteering a number you haven't sanity-checked invites an audit of every other figure); **missed the PII steelman entirely**; and *"dbt sees all the data since we're loading straight into the warehouse"* is wrong — dbt reads silver either way, so that isn't a gain from dropping bronze.

**Model answer:**
> "Bronze is a dependency boundary, not storage. GH Archive is a free third party with no SLA to me — if it 404s or shuts down, and bronze doesn't exist, I permanently lose the ability to reprocess anything. It's also my evidence: when a silver row looks wrong, bronze is the exact bytes I ingested, which is the only way to distinguish 'my transform broke' from 'the source changed.' And my bronze idempotency check is literally `blob.exists()` — no bucket, no mechanism. Cost is ~$42/year, which is noise. The real argument for dropping it is PII: GH Archive payloads carry author emails and bronze retains them indefinitely, so under a retention obligation I'd answer with a GCS lifecycle policy — Nearline at 30 days, delete at N — rather than removing the layer."

**Follow-ups to expect:**
- *"You're storing GitHub event payloads. Any reason you'd be **required** to delete bronze?"* ← the trap. **Author emails.** Answering "no" is worse than not raising it.
- *"When does direct-to-silver actually win?"* → when the source is *itself* a durable archive you control with an SLA (an internal Kafka topic with infinite retention, a vendor bucket you own), or at petabyte volume where duplication is genuinely expensive.
- *"Have you implemented the lifecycle policy?"* → **No.** Say so. Named, not built.

**Drill:** any time you state a cost, say the unit twice — *"three-fifty a month, forty-two a year."*

---

## Topic: Cost engineering

### Q: You claim this platform costs approximately $0. Walk me through every place it could have cost money and what you did about each.

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "Four places. **Query jobs** — BigQuery bills bytes scanned with a 10 MB floor, so silver is hour-partitioned, marts are pre-aggregated tables rather than views, and every API query job carries `maximum_bytes_billed=100 MB` as a hard stop. **Ingestion** — load jobs are free, streaming inserts are not, so I never stream. **Storage** — bronze is ~$42/year at a full year of GH Archive; the gold marts are tiny. **Idle infrastructure** — this is the big one: I `terraform destroy` and `docker compose down` at the end of every session, so nothing accrues overnight. And I picked the *API* deliberately per endpoint: `/runs` reads pipeline metadata through free `list_rows` rather than a query job, so my ops window costs literally nothing — I verified that against `bq ls -j` job history: three query jobs per dashboard load, never four."

**Follow-ups to expect:**
- *"What breaks that $0 the moment you deploy?"* → an unauthenticated public API over BigQuery is a **billing-DoS**: each request is a paid query job. That's why auth + rate limiting are the pre-deploy gate.
- *"Your caches — do they bound cost?"* → **Only partially, and be honest.** They protect against re-runs at the *same* key, not against exploring the key space: each new `limit` or `date` is 3 fresh query jobs at both layers. Mitigation named (invalidation key from `run_id`), not built.
- *"Cheapest thing you did?"* → choosing `list_rows` over a query job for `/runs`. Same data, zero bytes billed.

**Drill:** be able to name the *access path* for every read in the project and whether it bills. That distinction — which BigQuery **API** a read rides — is the answer that separates you.

---

### Q: Your dashboard has two caches in series. What's your worst-case staleness?

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "It's per-endpoint, not global — which is the part people get wrong. The three mart endpoints sit behind Streamlit's 300-second cache *and* the API's own 300-second TTL, so worst case is the **sum**, about 600 seconds. `/runs` only has the Streamlit layer at 30 seconds, because it rides the free path where the driver is responsiveness rather than cost. The refresh button clears only the near layer — the API will happily re-serve its own cached rows behind it, and the button is labelled to say so. The honest fix I named but didn't build is a single invalidation key derived from the latest `run_id` in `pipeline_run_metadata`, propagated as a cache-busting parameter, which collapses both layers to one truth."

**Follow-ups to expect:**
- *"Why different TTLs?"* → different **drivers**: 300 s on marts is a *cost* control (billed query jobs); 30 s on `/runs` is a *responsiveness* choice on a free path.
- *"Why is the cache in `app.py` and not the client?"* → `st.cache_data` is Streamlit-lifecycle memoisation; putting it in `api_client.py` would couple the client to Streamlit. The venv split makes that boundary a **failing test** rather than a convention.
