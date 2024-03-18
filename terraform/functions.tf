resource "google_cloudfunctions2_function" "event_handler" {
  name        = "event-handler"
  location    = var.region
  description = "A function for handling events from Octue services."

  build_config {
    runtime     = "python312"
    entry_point = "store_pub_sub_event_in_bigquery"
    source {
      storage_source {
        bucket = "twined-gcp"
        object = "event_handler_function_source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 100
    available_memory   = "256M"
    timeout_seconds    = 60
    environment_variables = {
      BIGQUERY_EVENTS_TABLE = "${google_bigquery_dataset.test_dataset.dataset_id}.${google_bigquery_table.test_table.table_id}"
    }
  }

}


resource "google_cloud_run_service_iam_member" "function_invoker" {
  location = google_cloudfunctions2_function.event_handler.location
  service  = google_cloudfunctions2_function.event_handler.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}


output "function_uri" {
  value = google_cloudfunctions2_function.event_handler.service_config[0].uri
}
