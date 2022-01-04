import yaml


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


class ConfigurationReader:
    def __init__(self, octue_configuration_path):
        self.octue_configuration_path = octue_configuration_path

        with open(self.octue_configuration_path) as f:
            self._configuration = yaml.load(f, Loader=yaml.SafeLoader)

    def create_knative_config_file(self, output_path):
        knative_configuration = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {"name": self._configuration["name"]},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "image": self._configuration.get("image_uri", DEFAULT_IMAGE_URI),
                                "resources": {
                                    "limits": {
                                        "memory": self._configuration.get("memory", "128Mi"),
                                        "cpu": self._configuration.get("cpus", 1),
                                    }
                                },
                            },
                            {"env": self._configuration.get("environment_variables", [])},
                        ],
                        "timeoutSeconds": 3600,
                        "containerConcurrency": self._configuration.get("concurrency", 80),
                    },
                    "metadata": {
                        "annotations": {
                            "autoscaling.knative.dev/minScale": self._configuration.get("minimum_instances", 0),
                            "autoscaling.knative.dev/maxScale": self._configuration.get("maximum_instances", 10),
                        }
                    },
                }
            },
        }

        with open(output_path, "w") as f:
            yaml.safe_dump(knative_configuration, f)
