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
