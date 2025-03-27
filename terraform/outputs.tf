output "event_store_id" {
  value       = module.octue_twined_core.event_store_id
  description = "The full ID of the BigQuery table acting as the Octue Twined services event store."
}


output "storage_bucket_url" {
  value = module.octue_twined_core.storage_bucket_url
  description = "The `gs://` URL of the storage bucket used to store service inputs, outputs, and diagnostics."
}
