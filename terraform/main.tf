terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # No backend block: state is stored locally (terraform.tfstate, gitignored).
  # Production would use a remote GCS backend with state locking.
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Bronze layer: raw, immutable GH Archive files exactly as ingested.
resource "google_storage_bucket" "bronze" {
  name     = var.bucket_name
  location = var.region

  # One IAM model for the whole bucket; ACLs are legacy and error-prone.
  uniform_bucket_level_access = true

  # Bronze holds raw event data — never allow it to become public.
  public_access_prevention = "enforced"

  # Immutable, replayable source of truth: keep prior versions of any object.
  versioning {
    enabled = true
  }

  # Bound the cost of versioning: keep the 3 newest prior versions, drop older.
  lifecycle_rule {
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  # DEV ONLY: lets `terraform destroy` remove a non-empty bucket for clean
  # teardown. In production set this to false so TF can't delete live data.
  force_destroy = true
}

# Silver layer: cleaned/typed tables loaded from Spark, queried by dbt.
resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.dataset_id
  location    = var.region
  description = "DevPulse silver-layer tables (cleaned, typed events from Spark)."

  # DEV ONLY: lets `terraform destroy` drop the dataset even if it has tables.
  # In production set this to false so TF can't delete the warehouse.
  delete_contents_on_destroy = true
}

# Non-human identity the pipeline (ingestion + Spark) authenticates as.
resource "google_service_account" "pipeline" {
  account_id   = "devpulse-pipeline"
  display_name = "DevPulse pipeline (ingestion + Spark)"
}

# --- Least-privilege IAM: each grant scoped as narrowly as the job allows ---
# _iam_member is additive (one role↔principal); it never clobbers other IAM.

# Manage objects in the bronze bucket only (not bucket config/IAM, not others).
resource "google_storage_bucket_iam_member" "pipeline_bronze_objects" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# Create/load tables in the silver dataset only.
resource "google_bigquery_dataset_iam_member" "pipeline_silver_editor" {
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# Run BigQuery load/query jobs. Must be project-scoped (no dataset-level job
# permission exists), but grants no data access on its own.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
