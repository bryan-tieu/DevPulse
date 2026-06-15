variable "project_id" {
  type        = string
  description = "GCP project ID that owns the DevPulse resources."
}

variable "region" {
  type        = string
  description = "Region for the GCS bucket and BigQuery dataset. Keep both co-located to avoid cross-region scan/egress costs."
}

variable "bucket_name" {
  type        = string
  description = "Globally-unique name for the GCS bronze bucket."
}

variable "dataset_id" {
  type        = string
  description = "BigQuery dataset ID for the silver-layer tables (letters/numbers/underscores only)."
}
