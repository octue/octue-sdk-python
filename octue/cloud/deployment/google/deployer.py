import subprocess
import tempfile
import uuid
import yaml

from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
from octue.cloud.pub_sub.topic import Topic
from octue.resources.service_backends import GCPPubSubBackend


DOCKER_REGISTRY_URL = "eu.gcr.io"


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

        with open(self.octue_configuration_path) as f:
            octue_configuration = yaml.load(f, Loader=yaml.SafeLoader)

        # Required configuration file entries.
        self.name = octue_configuration["name"]
        self.repository_name = octue_configuration["repository_name"]
        self.repository_owner = octue_configuration["repository_owner"]
        self.project_name = octue_configuration["project_name"]
        self.region = octue_configuration["region"]

        # Generated attributes.
        self.service_id = f"{OCTUE_NAMESPACE}.{uuid.uuid4()}"
        self.build_trigger_description = f"Build {self.name} service and deploy it to Cloud Run."

        self._default_image_uri = (
            f"{DOCKER_REGISTRY_URL}/{self.project_name}/{self.repository_owner}/{self.repository_name}/"
            f"{self.name}:{self._get_short_head_commit_hash()}"
        )

        # Optional configuration file entries.
        self.minimum_instances = octue_configuration.get("minimum_instances", 0)
        self.maximum_instances = octue_configuration.get("maximum_instances", 10)
        self.concurrency = octue_configuration.get("concurrency", 80)
        self.image_uri = octue_configuration.get("image_uri", self._default_image_uri)

        self.branch_pattern = octue_configuration.get("branch_pattern", "^main$")
        self.memory = octue_configuration.get("memory", "128Mi")
        self.cpus = octue_configuration.get("cpus", 1)
        self.environment_variables = octue_configuration.get("environment_variables", [])

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

    @staticmethod
    def _get_short_head_commit_hash():
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True).stdout.decode().strip()

    def _generate_cloud_build_configuration(self, with_cache=False):
        if not with_cache:
            cache_option = ["--no-cache"]
        else:
            cache_option = []

        environment_variables = ",".join(
            [f"{variable['name']}={variable['value']}" for variable in self.environment_variables]
            + [f"SERVICE_ID={self.service_id}"]
        )

        self.cloud_build_configuration = {
            "steps": [
                {
                    "id": "Build image",
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", *cache_option, "-t", self.image_uri, ".", "-f", "Dockerfile"],
                },
                {
                    "id": "Push image",
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["push", self.image_uri],
                },
                {
                    "id": "Deploy image to Google Cloud Run",
                    "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
                    "entrypoint": "gcloud",
                    "args": [
                        "run",
                        "services",
                        "update",
                        self.name,
                        "--platform=managed",
                        f"--image={self.image_uri}",
                        f"--region={self.region}",
                        f"--memory={self.memory}",
                        f"--cpu={self.cpus}",
                        f"--set-env-vars={environment_variables}",
                        "--timeout=3600",
                        f"--concurrency={self.concurrency}",
                        f"--min-instances={self.minimum_instances}",
                        f"--max-instances={self.maximum_instances}",
                        "--ingress=internal",
                    ],
                },
            ],
            "images": [self.image_uri],
        }

    def _create_build_trigger(self, cloud_build_configuration_path):
        command = [
            "gcloud",
            "beta",
            "builds",
            "triggers",
            "create",
            "github",
            f"--name={self.name}",
            f"--repo-name={self.repository_name}",
            f"--repo-owner={self.repository_owner}",
            f"--inline-config={cloud_build_configuration_path}",
            f"--description={self.build_trigger_description}",
            f"--branch-pattern={self.branch_pattern}",
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
        service = Service(backend=GCPPubSubBackend(project_name=self.project_name), service_id=self.service_id)
        topic = Topic(name=self.service_id, namespace=OCTUE_NAMESPACE, service=service)
        topic.create()

        command = [
            "gcloud",
            "beta",
            "eventarc",
            "triggers",
            "create",
            f"{self.name}-trigger",
            "--matching-criteria=type=google.cloud.pubsub.topic.v1.messagePublished",
            f"--destination-run-service={self.name}",
            f"--location={self.region}",
            f"--transport-topic={topic.name}",
        ]

        self._run_command(command)

    @staticmethod
    def _run_command(command):
        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise subprocess.SubprocessError(process.stderr.decode())
