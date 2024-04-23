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
        object = "event_handler/0.5.0.zip"
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

 event_trigger {
   trigger_region = var.region
   event_type = "google.cloud.pubsub.topic.v1.messagePublished"
   pubsub_topic = google_pubsub_topic.services_topic.id
   retry_policy = "RETRY_POLICY_RETRY"
 }

}


resource "google_cloud_run_service_iam_member" "function_invoker" {
  location = google_cloudfunctions2_function.event_handler.location
  service  = google_cloudfunctions2_function.event_handler.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
