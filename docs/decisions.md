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
