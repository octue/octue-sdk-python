import subprocess
import tempfile
import yaml


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


class Deployer:
    def __init__(
        self,
        octue_configuration_path,
        project_id,
        repository_name,
        repository_owner,
        description=None,
    ):
        self.octue_configuration_path = octue_configuration_path
        self.project_id = project_id
        self.repository_name = repository_name
        self.repository_owner = repository_owner
        self.description = description

        self._load_octue_configuration()

    def deploy(self):
        self._create_cloud_build_config()
        self._create_build_trigger()
        self._create_eventarc_run_trigger()

    def _load_octue_configuration(self):
        with open(self.octue_configuration_path) as f:
            self.octue_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        if self.octue_configuration.get("branch_pattern") and self.octue_configuration.get("pull_request_pattern"):
            raise ValueError("Only one of `branch_pattern` and `pull_request_pattern` can be provided in `octue.yaml`.")

    def _create_cloud_build_config(self, with_cache=False):
        if not with_cache:
            cache_option = ["--no-cache"]
        else:
            cache_option = []

        environment_variables = ",".join(
            [
                f"{variable['name']}={variable['value']}"
                for variable in self.octue_configuration.get("environment_variables", [])
            ]
        )

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
                        f'--image={self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI)}',
                        f"--region={self.octue_configuration['region']}",
                        f"--memory={self.octue_configuration.get('memory', '128Mi')}",
                        f"--cpu={self.octue_configuration.get('cpus', 1)}",
                        f"--set-env-vars={environment_variables}",
                        "--timeout=3600",
                        f"--concurrency={self.octue_configuration.get('concurrency', 80)}",
                        f"--min-instances={self.octue_configuration.get('minimum_instances', 0)}",
                        f"--max-instances={self.octue_configuration.get('maximum_instances', 10)}",
                        "--ingress=internal",
                    ],
                },
            ],
            "images": [self.octue_configuration.get("image_uri", DEFAULT_IMAGE_URI)],
        }

    def _create_build_trigger(self):
        if self.octue_configuration["branch_pattern"]:
            pattern_args = [f"--branch-pattern={self.octue_configuration['branch_pattern']}"]
        else:
            pattern_args = [f"--pull-request-pattern={self.octue_configuration['pull_request_pattern']}"]

        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                yaml.dump(self.cloud_build_configuration, f)

            command = [
                "gcloud",
                "beta",
                "builds",
                "triggers",
                "create",
                "github",
                f"--name={self.octue_configuration['name']}",
                f"--repo-name={self.repository_name}",
                f"--repo-owner={self.repository_owner}",
                f"--inline-config={temporary_file.name}",
                *pattern_args,
            ]

            process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise subprocess.SubprocessError(process.stderr.decode())

        print(process.stdout.decode() + process.stderr.decode())

    def _create_eventarc_run_trigger(self):
        command = [
            "gcloud",
            "beta",
            "eventarc",
            "triggers",
            "create",
            f"{self.octue_configuration['name']}-trigger",
            "--matching-criteria=type=google.cloud.pubsub.topic.v1.messagePublished",
            f"--destination-run-service={self.octue_configuration['name']}",
            f"--location={self.octue_configuration['region']}",
        ]

        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise subprocess.SubprocessError(process.stderr.decode())

        print(process.stdout.decode() + process.stderr.decode())
