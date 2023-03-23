variable "organization" {
  type = string
  default = "octue"
}

variable "project" {
  type    = string
  default = "octue-sdk-python"
}

variable "project_number" {
  type = string
  default = "437801218871"
}

variable "region" {
  type = string
  default = "europe-west1"
}

variable "github_organisation" {
  type    = string
  default = "octue"
}

variable "credentials_file" {
  type    = string
  default = "gcp-credentials.json"
}

variable "service_namespace" {
  type    = string
  default = "octue"
}

variable "service_name" {
  type    = string
  default = "example-service-cloud-run"
}
