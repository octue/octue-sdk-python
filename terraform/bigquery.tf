resource "google_bigquery_dataset" "test_dataset" {
  dataset_id                  = "octue_sdk_python_test_dataset"
  description                 = "A dataset for testing BigQuery subscriptions for the Octue SDK."
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "test_table" {
  dataset_id = google_bigquery_dataset.test_dataset.dataset_id
  table_id   = "question-events"

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {
    "name": "subscription_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "message_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "publish_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "data",
    "type": "JSON",
    "mode": "REQUIRED",
    "description": "Octue service event (e.g. heartbeat, log record, result)."
  },
  {
    "name": "attributes",
    "type": "JSON",
    "mode": "REQUIRED",
    "description": "Metadata for routing the event, adding context, and guiding the receiver's behaviour."
  }
]
EOF
}
