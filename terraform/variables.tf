variable "google_cloud_project_id" {
  type = string
  default = "octue-sdk-python"
}


variable "google_cloud_region" {
  type = string
  default = "europe-west9"
}


variable "github_organisation" {
  type = string
  default = "octue"
}


variable "developer_service_account_names" {
  type = set(string)
  default = ["cortadocodes", "thclark"]
}


variable "deletion_protection" {
  type    = bool
  default = false
}
