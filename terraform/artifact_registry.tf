resource "google_artifact_registry_repository" "artifact_registry_repository" {
  location      = var.region
  repository_id = "${var.service_namespace}"
  description   = "Docker image repository"
  format        = "DOCKER"
}
