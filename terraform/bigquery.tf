resource "google_bigquery_dataset" "test_dataset" {
  dataset_id                  = "octue_sdk_python_test_dataset"
  description                 = "A dataset for testing storing events for the Octue SDK."
  location                    = "EU"

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
    "name": "event",
    "type": "JSON",
    "mode": "REQUIRED"
  },
  {
    "name": "attributes",
    "type": "JSON",
    "mode": "REQUIRED"
  },
  {
    "name": "uuid",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "originator",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "sender",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "sender_type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "sender_sdk_version",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "recipient",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "question_uuid",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "order",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "backend",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "backend_metadata",
    "type": "JSON",
    "mode": "REQUIRED"
  }
]
EOF
}
