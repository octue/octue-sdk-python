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
