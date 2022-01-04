import yaml


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


class OctueConfigurationReader:
    def __init__(self, octue_configuration_path):
        self.octue_configuration_path = octue_configuration_path

        with open(self.octue_configuration_path) as f:
            self.octue_configuration = yaml.load(f, Loader=yaml.SafeLoader)

    def create_knative_config_file(self):
        self.knative_configuration = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {"name": self.octue_configuration["name"]},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "image": self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI),
                                "resources": {
                                    "limits": {
                                        "memory": self.octue_configuration.get("memory", "128Mi"),
                                        "cpu": self.octue_configuration.get("cpus", 1),
                                    }
                                },
                            },
                            {"env": self.octue_configuration.get("environment_variables", [])},
                        ],
                        "timeoutSeconds": 3600,
                        "containerConcurrency": self.octue_configuration.get("concurrency", 80),
                    },
                    "metadata": {
                        "annotations": {
                            "autoscaling.knative.dev/minScale": self.octue_configuration.get("minimum_instances", 0),
                            "autoscaling.knative.dev/maxScale": self.octue_configuration.get("maximum_instances", 10),
                        }
                    },
                }
            },
        }

    def create_cloud_build_config_file(self, with_cache=False):
        if not with_cache:
            cache_option = ["--no-cache"]
        else:
            cache_option = []

        self.cloud_build_configuration = {
            "steps": [
                {
                    "id": "Build image",
                    "name": "gcr.io/cloud-builders/docker",
                    "args": [
                        "build",
                        *cache_option,
                        "-t",
                        self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI),
                        ".",
                        "-f",
                        "Dockerfile",
                    ],
                },
                {
                    "id": "Push image",
                    "name": "gcr.io/cloud-builders/docker",
                    "args": [
                        "push",
                        self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI),
                    ],
                },
                {
                    "id": "Deploy image to Google Cloud Run",
                    "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
                    "entrypoint": "gcloud",
                    "args": [
                        "run",
                        "services",
                        "update",
                        self.octue_configuration["name"],
                        "--platform=managed",
                        f'--image={self.octue_configuration["image_uri"]}',
                        f"--region={self.octue_configuration['region']}",
                        "--quiet",
                    ],
                },
            ],
            "images": [self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI)],
        }
