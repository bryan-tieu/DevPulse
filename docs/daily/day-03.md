# Day 3 — The thin vertical slice (GH Archive → GCS bronze → BigQuery → FastAPI)

> Phase 0 · Week 1. Picks up from Day 2 (Terraform foundation — applied, verified, then `destroy`ed for cost). The repo still has only `docs/` and `terraform/`; today we create `ingestion/` and `api/` and prove the *whole pipe* end-to-end — ugly and manual, but real. This is the Phase 0 milestone.

## Environment notes (Windows 10 · PowerShell · VSCode)
The shell is **PowerShell**, not bash — several blueprint-style commands don't translate. Lessons banked from this session:
- **`terraform` is installed via winget** but had no PATH shim. Fixed permanently by appending the package dir to the User PATH: `...\AppData\Local\Microsoft\WinGet\Packages\Hashicorp.Terraform_Microsoft.Winget.Source_8wekyb3d8bbwe`. If `terraform` isn't found, the fix is a **full VSCode restart** (integrated terminals cache the environment from when VSCode launched — a new tab is not enough).
- **`curl` is an alias for `Invoke-WebRequest`** and rejects curl flags like `-O`. Use **`curl.exe`** (real curl, ships with Win10) or `Invoke-WebRequest -OutFile`.
- **No `gzip` / `gunzip` / `head` / `wc`** in PowerShell. Inspect `.gz` data with a **Python snippet** instead (you'll reuse the code in ingestion anyway).
- `gcloud` works normally. The GCP project is **`devpulse-dp2622`** — if the console shows no project, you're signed into the wrong Google account; confirm with `gcloud projects list`.

## Focus
Stand up one end-to-end pass — download a single GH Archive hour, land it raw in GCS bronze, do a trivial transform, load one table into BigQuery, and serve one row from a FastAPI endpoint — so every later layer plugs into something that already works.

## Build steps
Execute in order; one small commit per step so each diff is reviewable. Steps 1–2 are setup, not commits.

1. **Recreate infra + confirm auth.** Day 2 destroyed the cloud. In a terminal where `terraform -version` works:
   ```powershell
   terraform -chdir=terraform apply        # read the plan, then approve
   terraform -chdir=terraform output       # grab bucket_name + dataset_id for your .env
   gcloud auth application-default login    # only if ADC has expired
   ```
   The slice authenticates as **me via ADC**, not the pipeline SA (no SA key minted; see decisions).
2. **Look at the data first.** Download one hour, then inspect it in Python (no `gzip` in PowerShell):
   ```powershell
   curl.exe -O https://data.gharchive.org/2024-01-01-15.json.gz
   ```
   ```python
   import gzip, json
   from collections import Counter
   with gzip.open("2024-01-01-15.json.gz", "rt", encoding="utf-8") as f:
       events = [json.loads(line) for line in f]
   print(len(events), "events")
   print(json.dumps(events[0], indent=2)[:1500])          # one event, readable
   print(Counter(e["type"] for e in events).most_common()) # this IS a preview of the Step-5 transform
   ```
   Understand the shape (`type`, `actor`, `repo`, `created_at`, `payload`) before coding. The clean fields are `type` / `repo.name` / `created_at`; `payload` is the swamp Spark untangles later. The `.gz` is local scratch — keep it out of git. Do **not** query the GH Archive BigQuery public dataset.
3. **Project scaffolding.** Create `ingestion/` and `api/` packages (`__init__.py` each), a `requirements.txt` (`requests`, `google-cloud-storage`, `google-cloud-bigquery`, `fastapi`, `uvicorn[standard]`, `python-dotenv`), and `ruff` + `black` config. Put the TF output values in a **`.env`** (already gitignored): `GCP_PROJECT`, `BRONZE_BUCKET`, `BQ_DATASET`. Read config from env — no hardcoded names. *Commit.*
4. **Ingestion → bronze.** `ingestion/ingest.py`: download one hourly `.json.gz`, upload to GCS bronze **unchanged** at key `date=YYYY-MM-DD/hour=HH/<file>.json.gz`. Idempotent: `blob.exists()` → skip; bronze is immutable. Type hints, no bare `except`, `raise_for_status()` to fail loudly. *Commit.*
5. **Trivial transform → one BQ table.** Read the bronze object, count events by `type` (the `Counter` from step 2), load one silver table `hourly_event_counts` with an **explicit schema** and **WRITE_TRUNCATE** (re-run replaces, never appends → idempotent). Intentionally trivial — the real explode/dedupe/typing is the Phase 1 PySpark job. *Commit.*
6. **One FastAPI endpoint.** `api/main.py` with a single endpoint (`/event-counts`) that queries the BQ table and returns rows as JSON. `SELECT` only needed columns — BigQuery bills by bytes scanned. Run `uvicorn api.main:app --reload`; auto-docs at `/docs`. *Commit.*
7. **Run it end-to-end.** Ingest → transform → load → `uvicorn` → hit the endpoint and confirm a real row comes back. Then **re-run ingest + load** and confirm no duplicate object and identical counts (idempotency proof). The pipe is the deliverable, not any one piece.
8. **Cost hygiene + status.** `terraform -chdir=terraform destroy` at session end. Tick the Phase 0 slice box in `CLAUDE.md`, log any new choices in `decisions.md`, and check off the done criteria below.

## Constraints & conventions
- 🔐 **Least-privilege / no new secrets.** Run locally as me via **ADC** — do not mint a long-lived SA key for the slice. The pipeline SA exists for when Airflow/Spark need a non-human identity later. `.gitignore` already covers `*.json`, `.env`, state, tfvars, `*.gz` — verify nothing sensitive is staged before each commit.
- 📦 **Small, reviewable commits.** One logical step per commit, imperative messages (e.g. `add GH Archive ingestion to GCS bronze`). No giant "the whole slice" commit.
- 🧠 **Understand, don't rubber-stamp.** For each piece I want the *why* and the rejected alternative before it lands — especially the bronze partition layout, WRITE_TRUNCATE-for-idempotency, and querying-cost choices. Flag what's simplified vs. production (trivial transform now, Spark later).
- 🔁 **Idempotency is mandatory.** Re-running ingestion or the load must not duplicate or corrupt data — skip-if-exists on the bronze key, WRITE_TRUNCATE the table.
- 💸 **Cost hygiene.** Keep bucket + dataset co-located; one small file scanned is ~$0, but practice the discipline. `terraform destroy` at session end. The slice requires `apply` again at the *start* of any future run.
- 🚫 **Build the ingestion yourself.** Raw `.json.gz` from `data.gharchive.org` only — never the GH Archive BigQuery public dataset.

## Done criteria
- [X] `terraform apply` succeeded and outputs (bucket, dataset) are in `.env`, not hardcoded.
- [X] One GH Archive hour landed **unchanged** in GCS bronze under a `date=/hour=` partition key.
- [X] Re-running ingestion produces no duplicate object (idempotent).
- [X] One silver table exists in BigQuery, loaded with WRITE_TRUNCATE from the bronze file.
- [X] FastAPI serves one endpoint that returns a real row from that table; `/docs` renders.
- [X] End-to-end run verified by hand (ingest → load → API response), including the idempotency re-run.
- [X] No secrets/state/tfvars/`.gz` tracked in git. _(Commits still pending — code is written but uncommitted by request.)_
- [X] `terraform destroy` run; Phase 0 slice box ticked in `CLAUDE.md`; any new choices logged in `decisions.md`.

## Learning goals
1. **The medallion data flow, concretely** — why bronze stays raw/immutable, why the transform is separate, and what each layer hands the next.
2. **Idempotent ingestion + loads** — skip-if-exists object writes and WRITE_TRUNCATE vs. append, and why re-runnability is non-negotiable.
3. **GCS + BigQuery client basics from Python** — uploading objects, loading a table with an explicit schema, and querying with bytes-scanned cost awareness.
4. **Thin-slice discipline** — why we wire the whole pipe ugly first instead of perfecting the silver layer, and what "production-shaped" means at this stage (ADC vs. SA, trivial transform vs. Spark).
