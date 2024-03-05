# You need to start with a service account called "terraform" which has both the 'editor' and 'owner' basic permissions.
# This allows it to assign permissions to resources per https://cloud.google.com/iam/docs/understanding-roles
#
# To create domain-named storage buckets using terraform, you first have to verify ownership of the root domain, or
# "property", (eg octue.com) using the google search console. Once verified, you need to add the service account with
# which terraform acts ( eg terraform@octue-sdk-python.iam.gserviceaccount.com ) to Google Search Console > Settings > Users
# and Permissions, with "Owner" level permission.

resource "google_service_account" "dev_cortadocodes_service_account" {
    account_id   = "dev-cortadocodes"
    description  = "Allow cortadocodes to access developer-specific resources"
    display_name = "dev-cortadocodes"
    project      = var.project
}


resource "google_service_account" "github_actions_service_account" {
    account_id   = "github-actions"
    description  = "Allow GitHub Actions to test the SDK."
    display_name = "github-actions"
    project      = var.project
}


resource "google_project_iam_binding" "iam_serviceaccountuser" {
  project = var.project
  role    = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${google_service_account.dev_cortadocodes_service_account.email}",
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


resource "google_project_iam_binding" "pubsub_editor" {
  project = var.project
  role    = "roles/pubsub.editor"
  members = [
    "serviceAccount:${google_service_account.dev_cortadocodes_service_account.email}",
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


# Allows the GHA to call "namespaces get" for Cloud Run to determine the resulting run URLs of the services.
# This should also allow a service to get its own name by using:
#   https://stackoverflow.com/questions/65628822/google-cloud-run-can-a-service-know-its-own-url/65634104#65634104
resource "google_project_iam_binding" "run_developer" {
  project = var.project
  role    = "roles/run.developer"
  members = [
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


resource "google_project_iam_binding" "artifactregistry_writer" {
  project = var.project
  role    = "roles/artifactregistry.writer"
  members = [
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


resource "google_project_iam_binding" "storage_objectadmin" {
  project = var.project
  role = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


resource "google_project_iam_binding" "errorreporting_writer" {
  project = var.project
  role = "roles/errorreporting.writer"
  members = [
    "serviceAccount:${google_service_account.dev_cortadocodes_service_account.email}",
    "serviceAccount:${google_service_account.github_actions_service_account.email}",
  ]
}


resource "google_project_iam_binding" "bigquery_dataeditor" {
  project = var.project
  role = "roles/bigquery.dataEditor"
  members = [
    "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com",
  ]
}


resource "google_iam_workload_identity_pool" "github_actions_pool" {
    display_name              = "github-actions-pool"
    project                   = var.project
    workload_identity_pool_id = "github-actions-pool"
}


resource "google_iam_workload_identity_pool_provider" "github_actions_provider" {
    attribute_mapping                  = {
        "attribute.actor"            = "assertion.actor"
        "attribute.repository"       = "assertion.repository"
        "attribute.repository_owner" = "assertion.repository_owner"
        "google.subject"             = "assertion.sub"
    }
    display_name                       = "Github Actions Provider"
    project                            = var.project_number
    workload_identity_pool_id          = "github-actions-pool"
    workload_identity_pool_provider_id = "github-actions-provider"

    oidc {
        allowed_audiences = []
        issuer_uri        = "https://token.actions.githubusercontent.com"
    }
}

data "google_iam_policy" "github_actions_workload_identity_pool_policy" {
  binding {
    role = "roles/iam.workloadIdentityUser"
    members = [
      "principalSet://iam.googleapis.com/projects/${var.project_number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.github_actions_pool.workload_identity_pool_id}/attribute.repository_owner/${var.github_organisation}"
    ]
  }
}


# Allow a machine under Workload Identity Federation to act as the given service account.
resource "google_service_account_iam_policy" "github_actions_workload_identity_service_account_policy" {
  service_account_id = google_service_account.github_actions_service_account.name
  policy_data        = data.google_iam_policy.github_actions_workload_identity_pool_policy.policy_data
}
