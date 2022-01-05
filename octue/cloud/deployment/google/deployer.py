import subprocess
import tempfile
import uuid
import yaml

from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
from octue.cloud.pub_sub.topic import Topic
from octue.resources.service_backends import GCPPubSubBackend


DEFAULT_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:latest"


class ProgressMessage:
    def __init__(self, message, stage, total_number_of_stages):
        self.message = f"[{stage}/{total_number_of_stages}] {message}..."

    def __enter__(self):
        print(self.message, end="", flush=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            print("done.")


class Deployer:
    def __init__(self, octue_configuration_path):
        self.octue_configuration_path = octue_configuration_path
        self.service_id = f"{OCTUE_NAMESPACE}.{uuid.uuid4()}"
        self._load_octue_configuration()

        self.project_id = self.octue_configuration["project_name"]
        self.repository_name = self.octue_configuration["repository_name"]
        self.repository_owner = self.octue_configuration["repository_owner"]
        self.description = f"Build {self.octue_configuration['name']} service and deploy it to Cloud Run."

    def deploy(self, no_cache):
        total_number_of_stages = 4

        with ProgressMessage("Generating Google Cloud Build configuration", 1, total_number_of_stages):
            self._generate_cloud_build_configuration(with_cache=not no_cache)

        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                yaml.dump(self.cloud_build_configuration, f)

            with ProgressMessage("Creating build trigger", 2, total_number_of_stages):
                self._create_build_trigger(cloud_build_configuration_path=temporary_file.name)

            with ProgressMessage("Building and deploying service", 3, total_number_of_stages):
                self._build_and_deploy_service(cloud_build_configuration_path=temporary_file.name)

        with ProgressMessage("Creating and attaching Eventarc Pub/Sub run trigger", 4, total_number_of_stages):
            self._create_eventarc_run_trigger()

        print(f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}.")

        return self.service_id

    def _load_octue_configuration(self):
        with open(self.octue_configuration_path) as f:
            self.octue_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        if self.octue_configuration.get("branch_pattern") and self.octue_configuration.get("pull_request_pattern"):
            raise ValueError("Only one of `branch_pattern` and `pull_request_pattern` can be provided in `octue.yaml`.")

    def _generate_cloud_build_configuration(self, with_cache=False):
        if not with_cache:
            cache_option = ["--no-cache"]
        else:
            cache_option = []

        environment_variables = ",".join(
            [
                f"{variable['name']}={variable['value']}"
                for variable in self.octue_configuration.get("environment_variables", [])
            ]
            + [f"SERVICE_ID={self.service_id}"]
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

    def _create_build_trigger(self, cloud_build_configuration_path):
        if self.octue_configuration["branch_pattern"]:
            pattern_args = [f"--branch-pattern={self.octue_configuration['branch_pattern']}"]
        else:
            pattern_args = [f"--pull-request-pattern={self.octue_configuration['pull_request_pattern']}"]

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
            f"--inline-config={cloud_build_configuration_path}",
            f"--description={self.description}",
            *pattern_args,
        ]

        self._run_command(command)

    def _build_and_deploy_service(self, cloud_build_configuration_path):
        command = [
            "gcloud",
            "builds",
            "submit",
            ".",
            f"--config={cloud_build_configuration_path}",
        ]

        self._run_command(command)

    def _create_eventarc_run_trigger(self):
        service = Service(backend=GCPPubSubBackend(project_name=self.project_id), service_id=self.service_id)
        topic = Topic(name=self.service_id, namespace=OCTUE_NAMESPACE, service=service)
        topic.create()

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
            f"--transport-topic={topic.name}",
        ]

        self._run_command(command)

    def _run_command(self, command):
        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise subprocess.SubprocessError(process.stderr.decode())
