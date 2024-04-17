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
  table_id   = "service-events"
  clustering = ["sender", "question_uuid"]

  schema = <<EOF
[
  {
    "name": "datetime",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "uuid",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "kind",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "event",
    "type": "JSON",
    "mode": "REQUIRED"
  },
  {
    "name": "other_attributes",
    "type": "JSON",
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
