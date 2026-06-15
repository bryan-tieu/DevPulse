# Day 2 — Terraform the cloud foundation (GCS + BigQuery + service account)

> Phase 0 · Week 1. Picks up from Day 1 (repo skeleton, `.venv`, GCP account + billing alerts). The `terraform/` dir already has empty `main.tf` / `variables.tf` / `outputs.tf` stubs — today we fill them.

## Focus
Provision the bronze GCS bucket, the BigQuery dataset, and a least-privilege pipeline service account as Terraform code — the cloud foundation every later layer plugs into.

## Build steps
Execute in order; one small commit per step so each diff is reviewable.

1. **Enable required GCP APIs.** Add `google_project_service` for `storage`, `bigquery`, and `iam` (idempotent; skip if Day 1 already enabled them). Confirm `gcloud auth application-default login` is done so Terraform can authenticate locally.
2. **Provider + state.** In `main.tf`, configure the `google` provider, pin the provider version, and read `project_id` / `region` from variables. Use **local state** for now (already gitignored) — note in `decisions.md` that production would use a remote GCS backend with state locking.
3. **Variables.** In `variables.tf`: `project_id`, `region`, `bucket_name`, `dataset_id` (typed, with descriptions; defaults only where safe). Create a **`terraform.tfvars`** for real values and confirm it's gitignored.
4. **GCS bronze bucket.** `google_storage_bucket`: uniform bucket-level access, `versioning` on, a lifecycle rule, and `force_destroy = true` (dev convenience for clean teardown — flag this as a dev-only choice). Co-locate region with the dataset.
5. **BigQuery dataset.** `google_bigquery_dataset` in the **same region** as the bucket (cross-region scans cost money).
6. **Service account + least-privilege IAM.** `google_service_account` for the pipeline, then **resource-scoped** bindings — `roles/storage.objectAdmin` on the bucket, `roles/bigquery.dataEditor` on the dataset, `roles/bigquery.jobUser` at project level (needed to run load jobs). No project-wide `editor`/`owner`.
7. **Outputs.** Export `bucket_name`, `dataset_id`, and `service_account_email` in `outputs.tf` — later layers consume these.
8. **Run the loop.** `terraform fmt` → `init` → `validate` → `plan`. **Read the plan together before applying** — walk each resource and each IAM binding so you can defend it. Then `apply`.
9. **Verify.** Confirm bucket + dataset + SA exist via `gcloud`/console, and that the SA has only the granted roles.
10. **Update status.** Tick the Phase 0 Terraform box in `CLAUDE.md`, record the local-state and `force_destroy` calls in `decisions.md`.

## Constraints & conventions
- 🔐 **Least-privilege IAM.** Resource-scoped roles only; never `editor`/`owner` on the project. If a step seems to need broad access, stop and find the narrow role.
- 📦 **Small, reviewable commits.** One logical step per commit, imperative messages (e.g. `add GCS bronze bucket via terraform`). No giant "all of terraform" commit.
- 🧠 **Understand, don't rubber-stamp.** For each resource and IAM binding, I want the *why* and the alternative before it lands — especially the `terraform plan` output. Explain production vs. simplified (local state, `force_destroy`) as we go.
- 💸 **Cost hygiene.** Bucket + dataset are ~$0 empty, but practice the discipline: `terraform destroy` at session end unless we're continuing tomorrow. Keep bucket and dataset co-located.
- 🚫 **Secrets stay out of git.** `.gitignore` already covers `*.json`, `.env`, `.terraform/`, `*.tfstate*`. Verify `terraform.tfvars` and any SA key are ignored before committing. Do **not** generate a long-lived SA key unless we decide we need one — prefer ADC locally.

## Done criteria
- [X] `terraform plan` is clean and `apply` succeeds.
- [X] GCS bronze bucket exists (uniform access + versioning), in the chosen region.
- [X] BigQuery dataset exists in the **same** region.
- [X] Pipeline service account exists with **only** resource-scoped least-privilege roles.
- [X] `outputs.tf` exports bucket, dataset, and SA email.
- [X] No secrets/state/tfvars tracked in git (`git status` clean of them).
- [X] Phase 0 Terraform box ticked in `CLAUDE.md`; choices logged in `decisions.md`.
- [ ] `terraform destroy` run (or a conscious decision to leave resources up).

## Learning goals
1. **Terraform workflow & state** — what `init/plan/apply` each do, why state exists, and the local-vs-remote-backend tradeoff.
2. **Least-privilege IAM on GCP** — resource-scoped vs. project-scoped roles, and why `jobUser` is separate from `dataEditor`.
3. **GCS + BigQuery as a lakehouse foundation** — buckets, uniform access, versioning, datasets, and why region co-location matters for cost.
4. **IaC discipline** — declarative provisioning, idempotency, and reproducible teardown/recreate as a cost-control habit.
