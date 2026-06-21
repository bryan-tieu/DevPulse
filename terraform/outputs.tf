output "bronze_bucket_name" {
  value       = google_storage_bucket.bronze.name
  description = "GCS bronze bucket name."
}

output "silver_bucket_name" {
  value       = google_storage_bucket.silver.name
  description = "GCS silver bucket name."
}

output "dataset_id" {
  value       = google_bigquery_dataset.silver.dataset_id
  description = "BigQuery silver dataset ID."
}

output "service_account_email" {
  value       = google_service_account.pipeline.email
  description = "Email of the pipeline service account."
}

