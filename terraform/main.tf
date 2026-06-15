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
