# DevPulse — A Production-Grade Data Engineering Platform
### Your 2-Month Blueprint to Data Engineer

> A complete, modern-data-stack project built deliberately to close every gap from your skills analysis: cloud, orchestration, distributed processing, warehousing, dimensional modeling, dbt, data quality, streaming, IaC, and CI/CD — while reusing your existing Python, SQL, Pandas, FastAPI, and data-cleaning strengths.

---

## 1. The Product (so every pipeline has a purpose)

**DevPulse** ingests the global firehose of GitHub activity and turns it into developer-ecosystem analytics: trending repositories, programming-language momentum over time, contributor leaderboards, and activity-spike detection. It serves these through an **API** and a **dashboard**.

This isn't a "tech demo" — it's framed as a real product. Every pipeline you build answers a question a real consumer would ask. That framing is what separates a portfolio project from a tutorial, and it's what you'll talk about in interviews.

### Why this data source

You'll use **GH Archive** (`data.gharchive.org`), which publishes **hourly `.json.gz` dumps of all public GitHub events** going back to 2011, plus the **live GitHub Events API** for near-real-time data. It's the single best source for this project because it naturally supports *every* layer of the stack:

| Need | What GH Archive gives you |
|---|---|
| Large historical batch (justifies Spark) | Years of hourly archives — terabytes if you want them |
| Recurring scheduled batch (justifies Airflow) | A new file every hour, on a predictable schedule |
| Real-time streaming (justifies Kafka) | Live Events API polled continuously |
| Genuine transform difficulty | Deeply nested, messy JSON with ~15 event types |
| A believable analytics product | Trends, leaderboards, momentum — all real questions |

> **Important:** GH Archive is *also* pre-loaded as a BigQuery public dataset. **Do not** just query that table — that skips the engineering. You ingest the raw files yourself. The whole point is building the pipeline.

**Swappable domains** (architecture is identical — pick what motivates you): live crypto trades via Coinbase/Binance WebSocket; the Bluesky firehose; transit GTFS-Realtime feeds; OpenSky flight data. If you swap, keep the same layered architecture below.

---

## 2. Architecture — Medallion Lakehouse

```
                        ┌─────────────── ORCHESTRATION (Airflow, in Docker) ───────────────┐
                        │   schedules • retries • backfills • sensors • SLAs • alerting      │
                        └───────────────────────────────────────────────────────────────────┘
                                  │              │                │              │
  SOURCES                  INGEST │       SILVER │ (PySpark)  GOLD │ (dbt)  SERVE │
  ┌──────────────┐         ┌──────▼──────┐  ┌─────▼──────┐   ┌──────▼─────┐  ┌────▼─────────┐
  │ GH Archive   │ batch   │ BRONZE      │  │ SILVER     │   │ GOLD       │  │ FastAPI      │
  │ hourly .gz   ├────────►│ raw .json.gz├─►│ cleaned,   ├──►│ star schema│─►│ /trending    │
  └──────────────┘         │ in GCS,     │  │ deduped,   │   │ facts+dims │  │ /languages   │
  ┌──────────────┐         │ partitioned │  │ Parquet    │   │ + marts in │  │ /leaderboard │
  │ Events API   │ stream  │ by date/hr  │  │ → BigQuery │   │ BigQuery   │  └──────────────┘
  │ (live)       ├──► Kafka └─────────────┘  └────────────┘   └────────────┘  ┌──────────────┐
  └──────────────┘   │                                                         │ Dashboard    │
                     └──► Spark Structured Streaming ──► real-time table ──────►│ (Streamlit)  │
                                                                               └──────────────┘

  CROSS-CUTTING:  Data Quality (Great Expectations + dbt tests)  •  IaC (Terraform)
                  Containerization (Docker Compose)  •  CI/CD (GitHub Actions)  •  Observability
```

**The medallion pattern** (bronze → silver → gold) is the industry-standard way to structure a lakehouse, and being able to explain *why* each layer exists is a strong interview signal:

- **Bronze** — raw, immutable, exactly as ingested. Never transformed. Your replayable source of truth.
- **Silver** — cleaned, deduplicated, typed, flattened. The heavy lifting. *This is your distributed-processing centerpiece (PySpark).*
- **Gold** — business-ready dimensional models and aggregate marts. *This is your warehouse + dbt work.*

---

## 3. The Stack (and why each choice)

| Layer | Tool | Why this one |
|---|---|---|
| Object storage (lake) | **Google Cloud Storage** | Generous free tier; the bronze/silver lake |
| Warehouse | **BigQuery** | Serverless (no clusters to manage), strong free tier (1 TB queries + 10 GB storage/mo), in huge job demand |
| Orchestration | **Apache Airflow** (Docker) | The default in industry; run it locally — you do **not** need expensive managed Airflow to prove the skill |
| Distributed processing | **PySpark** (Docker) | The silver-layer transform; the distributed paradigm you're missing |
| Transformation / modeling | **dbt-core** (on BigQuery) | The modern transformation standard; gives you tests, docs, lineage, and dimensional modeling |
| Streaming | **Apache Kafka** (Docker) + Spark Structured Streaming | Gold-tier resume keyword; demonstrates real-time ingestion |
| Data quality | **Great Expectations** + dbt tests | Validation gates between layers — a true production behavior |
| IaC | **Terraform** | Provisions GCS + BigQuery + IAM as code |
| Containerization | **Docker / Docker Compose** | Runs your whole local stack reproducibly |
| CI/CD | **GitHub Actions** | Lint, test, dbt build, deploy on every PR |
| Serving | **FastAPI** + Streamlit (or Looker Studio) | **Reuses your existing strength**; turns the warehouse into a product |

### Why GCP/BigQuery over AWS
AWS has slightly more job demand, but for a 2-month student project, GCP's serverless BigQuery + free tier means you **finish the project instead of fighting cluster config and surprise bills**. The *concepts* transfer 1:1 to AWS (S3↔GCS, Redshift/Athena↔BigQuery, Glue↔Dataflow). Learn the patterns here; pick up AWS-specific services after.
*Alternative:* **Snowflake** is enormously in-demand and has a 30-day free trial + $400 credits — viable, but the trial expiring mid-project is a risk. Stick with BigQuery unless you specifically want Snowflake on the resume.

### 💸 Cost control — read this before you touch the cloud
- Keep **Spark and Kafka local** in Docker — no managed clusters.
- Set a **BigQuery billing budget + alert** and a **byte-scanned quota** on day one.
- `terraform destroy` cloud resources when you're not actively working.
- Partition your data and `SELECT` only what you need — BigQuery bills by bytes scanned.
- Done right, this project costs **<$10 total**, often $0.

---

## 4. The 8-Week Plan

**Core principle: build a thin end-to-end slice first, then deepen each layer.** Do *not* perfect one layer before connecting the next — that's how 2-month projects die at 60% complete. Make it work ugly, then make it real.

**Assumed effort:** ~20 hrs/week (~160 hrs total). If you have less, that's fine — the plan is phased so you always have a *complete* project at the Week 6 finish line, with streaming as a stretch.

---

### Phase 0 — Setup & Thin Vertical Slice · Week 1
The goal of week 1 is a single ugly pipe that runs **end to end**, so every later piece plugs into something that already works.

- [ ] GitHub repo + clean project structure (`/ingestion`, `/spark`, `/dbt`, `/airflow`, `/api`, `/terraform`, `/tests`)
- [ ] GCP account; **set billing budget + alerts immediately**
- [ ] Terraform: provision one GCS bucket + one BigQuery dataset + a service account
- [ ] Docker Compose skeleton
- [ ] **Slice:** a script downloads ONE hourly GH Archive file → lands it in GCS bronze → a trivial transform → loads ONE table to BigQuery → one FastAPI endpoint returns a row
- ✅ **Milestone:** the whole pipe exists, manual and ugly, but real.

### Phase 1 — Batch Lakehouse Core · Weeks 2–3
- [ ] **Week 2 — Airflow:** stand up Airflow in Docker. Build the **ingestion DAG**: download each hourly archive → GCS bronze, partitioned by `date/hour`, **idempotent**. Add retries, a sensor that waits for the file to be available, and **backfill one week of history**.
- [ ] **Week 3 — PySpark (silver):** read the gzipped JSON from bronze, **explode the nested structures, dedupe** (events repeat across files), normalize the ~15 event types, cast types, write **partitioned Parquet** to silver, load to BigQuery. Wire this job into the Airflow DAG.
- ✅ **Milestone:** orchestrated batch ingestion + a distributed cleaning job, scheduled and backfillable.

### Phase 2 — Warehouse Modeling & Quality · Weeks 4–5
- [ ] **Week 4 — dbt (gold):** staging models → a **dimensional star schema**: `fact_events` (or `fact_daily_repo_activity`) + `dim_repo`, `dim_actor`, `dim_date`, `dim_event_type`. Make the fact an **incremental** model. Build marts: `trending_repos_daily`, `language_momentum`, `contributor_leaderboard`. Add dbt **tests** (not_null, unique, relationships, accepted_values) and generate **dbt docs** (lineage graph).
- [ ] **Week 5 — Data quality + observability:** Great Expectations gates between bronze→silver (schema, null-rate, row-count checks); make dbt tests a **gate** in the Airflow DAG so the pipeline **fails loudly** on bad data; add failure **alerting** (Slack/email); log **run metadata** (row counts, durations) to a small metadata table.
- ✅ **Milestone:** a tested, monitored, modeled warehouse — this is what "production" actually feels like.

### Phase 3 — Serving & CI/CD · Week 6 — 🏁 GUARANTEED FINISH LINE
- [ ] **FastAPI** over the gold marts: `/trending`, `/languages/{lang}/momentum`, `/repos/{id}`, `/leaderboard` — with pagination, caching, auto-docs.
- [ ] **Dashboard:** Streamlit or Looker Studio on the gold marts.
- [ ] **GitHub Actions CI:** ruff + black + sqlfluff lint, pytest on transform logic, `dbt build` against a CI dataset, GE checks on PR. **CD:** build/push Docker images (stretch: deploy the API to Cloud Run).
- ✅ **Milestone:** a COMPLETE, production-shaped batch platform. **If school crunches and you stop here, you already have a standout portfolio piece.**

### Phase 4 — Streaming (Stretch) · Weeks 7–8
- [ ] **Week 7 — Kafka:** Kafka in Docker. A containerized producer polls the live GitHub Events API → Kafka topic. **Spark Structured Streaming** consumes → micro-batches into bronze / a real-time table.
- [ ] **Week 8 — Real-time + polish:** a real-time mart (live activity feed / rolling trending), a real-time endpoint, then **architecture diagram, README, short demo video, and a "what I'd change at scale" writeup**. Buffer for overruns.
- ✅ **Milestone:** real-time ingestion on top of the batch platform — the full Lambda-ish architecture.

> **Honest descoping rule:** Weeks 1–6 are non-negotiable and constitute a *complete* project. Weeks 7–8 (streaming) are a stretch. If you fall behind, **drop streaming** and use the time to deepen data quality, observability, and polish instead. A finished, deep batch platform beats an unfinished one with Kafka bolted on.

---

## 5. Skills Map — Old + New

This project is engineered to hit **every gap** from your analysis while building on what you already have.

**Reused (your existing strengths, leveled up):**
Python · SQL (now in BigQuery + dbt) · Pandas/NumPy (validation & small transforms) · PostgreSQL knowledge → warehouse SQL · **FastAPI** (serving layer) · data cleaning at scale (the silver layer) · Git · Docker (extended).

**New (the gaps, now closed):**
☐ Cloud (GCS + BigQuery) · ☐ Airflow orchestration · ☐ PySpark / distributed processing · ☐ dbt · ☐ Dimensional modeling (star schema) · ☐ Data quality (Great Expectations) · ☐ Kafka / streaming · ☐ Terraform / IaC · ☐ GitHub Actions CI/CD · ☐ Medallion lakehouse architecture · ☐ Observability & alerting.

---

## 6. What "Production-Level" Honestly Means Here

A few things will be **simulated**, and knowing the difference is itself interview gold:

- Your Spark runs **single-node in Docker**, not a real cluster. That's fine — you should be able to *articulate* what changes at real scale (partitioning strategy, shuffles, executor tuning, why you'd move to Dataproc/EMR/Databricks). That articulation is worth as much as the code.
- You'll **touch** some tools enough to discuss them credibly, not master them — mastery comes on the job. Be honest about that; it's a strength, not a weakness.
- The patterns — medallion layering, idempotent ingestion, incremental models, test gates, IaC, CI/CD — are **exactly** what real shops do. The architecture is genuinely production-shaped even at small scale.

---

## 7. Deliverables That Make It Land

Build these alongside the code — they convert the project into interview ammunition:

- A **README** with the architecture diagram and a clear "what problem this solves."
- The **dbt docs site** (lineage graph) — visually impressive and rare in junior portfolios.
- A **2–3 min demo video** or screenshots of the API + dashboard + Airflow DAG.
- A **"design decisions & tradeoffs"** section: why medallion, why BigQuery, what you'd change at scale, what broke and how you fixed it.
- A clean **commit history** — it shows how you work.

---

## 8. First Three Moves (start today)

1. Create the repo and the folder skeleton.
2. Create a GCP account and **set a billing budget + alert** before anything else.
3. Manually download one GH Archive file (`https://data.gharchive.org/2024-01-01-15.json.gz`), open it, and read 50 events. Understand your data before you build anything around it.

Everything else follows from a working thin slice. Build the pipe first; make it beautiful later.
