import subprocess
import tempfile
import uuid
import yaml

from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
from octue.cloud.pub_sub.subscription import Subscription
from octue.cloud.pub_sub.topic import Topic
from octue.exceptions import DeploymentError
from octue.resources.service_backends import GCPPubSubBackend


DOCKER_REGISTRY_URL = "eu.gcr.io"

DEFAULT_DOCKERFILE_URL = (
    "https://raw.githubusercontent.com/octue/octue-sdk-python/main/octue/cloud/deployment/google/cloud_run/Dockerfile"
)


class CloudRunDeployer:
    """A tool for taking an `octue.yaml` file in a repository and deploying the repository's `octue` app to Google Cloud
    Run. This includes setting up a Google Cloud Build trigger, enabling automatic deployment during future development.
    Note that this tool requires the `gcloud` CLI to be available. The version used while developing this tool is:

    ```
    Google Cloud SDK 367.0.0
    beta 2021.12.10
    bq 2.0.72
    cloud-build-local 0.5.2
    core 2021.12.10
    gsutil 5.5
    pubsub-emulator 0.6.0
    ```

    :param str octue_configuration_path: the path to the `octue.yaml` file if it's not in the current working directory
    :param str|None service_id: the UUID to give the service if a random one is not wanted
    :return None:
    """

    def __init__(self, octue_configuration_path, service_id=None):
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
        self.service_id = f"{OCTUE_NAMESPACE}.{service_id or uuid.uuid4()}"
        self.build_trigger_description = f"Build {self.name} service and deploy it to Cloud Run."

        self._default_image_uri = (
            f"{DOCKER_REGISTRY_URL}/{self.project_name}/{self.repository_owner}/{self.repository_name}/"
            f"{self.name}:{self._get_short_head_commit_hash()}"
        )

        # Optional configuration file entries.
        self.dockerfile_path = octue_configuration.get("dockerfile_path")
        self.minimum_instances = octue_configuration.get("minimum_instances", 0)
        self.maximum_instances = octue_configuration.get("maximum_instances", 10)
        self.concurrency = octue_configuration.get("concurrency", 80)
        self.image_uri = octue_configuration.get("image_uri", self._default_image_uri)
        self.branch_pattern = octue_configuration.get("branch_pattern", "^main$")
        self.memory = octue_configuration.get("memory", "128Mi")
        self.cpus = octue_configuration.get("cpus", 1)
        self.environment_variables = octue_configuration.get("environment_variables", [])

    def deploy(self, no_cache=False, update=False):
        """Create a Google Cloud Build configuration from the `octue.yaml file, create a build trigger, build and
        deploy the app as a Google Cloud Run service, and create and attach an Eventarc Pub/Sub run trigger to it.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger and Eventarc run trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return str: the service's UUID
        """
        total_number_of_stages = 4

        with ProgressMessage("Generating Google Cloud Build configuration", 1, total_number_of_stages):
            self._generate_cloud_build_configuration(with_cache=not no_cache)

        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                yaml.dump(self.cloud_build_configuration, f)

            with ProgressMessage("Creating build trigger", 2, total_number_of_stages) as progress_message:
                try:
                    self._create_build_trigger(cloud_build_configuration_path=temporary_file.name)
                except DeploymentError as e:
                    self._raise_or_ignore_already_exists_error(e, update, progress_message)

            with ProgressMessage("Building and deploying service", 3, total_number_of_stages):
                self._build_and_deploy_service(cloud_build_configuration_path=temporary_file.name)

        with ProgressMessage("Creating Eventarc Pub/Sub run trigger", 4, total_number_of_stages) as progress_message:
            try:
                self._create_eventarc_run_trigger()
            except DeploymentError as e:
                self._raise_or_ignore_already_exists_error(e, update, progress_message)

        print(f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}.")

        return self.service_id

    @staticmethod
    def _get_short_head_commit_hash():
        """Get the short commit hash for the HEAD commit in the current git repository.

        :return str:
        """
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True).stdout.decode().strip()

    def _generate_cloud_build_configuration(self, with_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `cloud_build_configuration` attribute.

        :param bool with_cache: if `True`, specify in the configuration to use the docker cache when building the service image
        :return None:
        """
        if self.dockerfile_path:
            get_dockerfile_step = []
            dockerfile_path = self.dockerfile_path

        else:
            # If no path to a dockerfile has been provided, add a step to download the default Octue service Dockerfile
            # to build the image from.
            get_dockerfile_step = [
                {
                    "id": "Get default Octue Dockerfile",
                    "name": "alpine:latest",
                    "args": ["wget", DEFAULT_DOCKERFILE_URL],
                }
            ]

            dockerfile_path = "Dockerfile"

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
                *get_dockerfile_step,
                {
                    "id": "Build image",
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", *cache_option, "-t", self.image_uri, ".", "-f", dockerfile_path],
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
        """Create the build trigger in Google Cloud Build using the given `cloudbuild.yaml` file.

        :param str cloud_build_configuration_path:
        :return None:
        """
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
        """Build and deploy the service from the given cloud build configuration file and local context. This method
        must be run from the same directory that `docker build -f <path/to/Dockerfile .` would be run from locally for
        the correct build context to be available. When `gcloud beta builds triggers run` is working, this won't be
        necessary as the build context can just be taken from the relevant GitHub repository.

        :param str cloud_build_configuration_path:
        :return None:
        """
        build_and_deploy_command = [
            "gcloud",
            "builds",
            "submit",
            ".",
            f"--config={cloud_build_configuration_path}",
        ]

        self._run_command(build_and_deploy_command)

        allow_unauthenticated_messages_command = [
            "gcloud",
            "run",
            "services",
            "add-iam-policy-binding",
            self.name,
            f"--region={self.region}",
            "--member=allUsers",
            "--role=roles/run.invoker",
        ]

        self._run_command(allow_unauthenticated_messages_command)

    def _create_eventarc_run_trigger(self):
        """Create an Eventarc run trigger for the service and attach it. Update the Eventarc subscription to have the
        minimum acknowledgement deadline to avoid recurrent re-computation of questions.

        :raise octue.exceptions.DeploymentError: if the Eventarc subscription is not found after creating the Eventarc trigger
        :return None:
        """
        service = Service(backend=GCPPubSubBackend(project_name=self.project_name), service_id=self.service_id)
        topic = Topic(name=self.service_id, namespace=OCTUE_NAMESPACE, service=service)
        topic.create(allow_existing=True)

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

        eventarc_subscription_path = None

        for subscription_path in topic.get_subscriptions():
            if self.name in subscription_path:
                eventarc_subscription_path = subscription_path
                break

        if not eventarc_subscription_path:
            raise DeploymentError(
                "Eventarc subscription not found - its acknowledgement has not been updated to the minimum value."
            )

        # Set the acknowledgement deadline to the minimum value to avoid recurrent re-computation of questions.
        subscription = Subscription(
            name=eventarc_subscription_path.split("/")[-1],
            topic=topic,
            namespace="",
            project_name=self.project_name,
            ack_deadline=10,
        )

        subscription.update()

    @staticmethod
    def _run_command(command):
        """Run a command in a subprocess, raising a `DeploymentError` if it fails.

        :param iter(str) command: the command to run in `subprocess` form e.g. `["cat", "my_file.txt"]`
        :raise octue.exceptions.DeploymentError: if the command fails
        :return None:
        """
        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise DeploymentError(process.stderr.decode())

    @staticmethod
    def _raise_or_ignore_already_exists_error(exception, update, progress_message):
        """If `update` is `True` and the exception includes the words "already exists", ignore the exception and change
        the progress message's `finish_message` to "already exists."; otherwise, raise the exception.

        :param Exception exception: the exception to ignore or raise
        :param bool update: if `True`, ignore "already exists" errors
        :param ProgressMessage progress_message:
        :return None:
        """
        if update and "already exists" in exception.args[0]:
            progress_message.finish_message = "already exists."
        else:
            raise exception


class ProgressMessage:
    """A context manager that, on entering the context, prints the given start message and, on leaving it, prints
    "done" on the same line. The use case is to surround a block of code with a start and finish message to give an
    idea of progress on the command line. If multiple progress messages are required, different instances of this class
    can be used and given information on their ordering and the total number of stages to produce an enumerated output.

    For example:
    ```
    [1/4] Generating Google Cloud Build configuration...done.
    [2/4] Creating build trigger...done.
    [3/4] Building and deploying service...done.
    [4/4] Creating Eventarc Pub/Sub run trigger...done.
    ```

    :param str start_message: the message to print before the code in the context is executed
    :param int stage: the position of the progress message among all the related progress messages
    :param int total_number_of_stages: the total number of progress messages that will be printed
    :return None:
    """

    def __init__(self, start_message, stage, total_number_of_stages):
        self.start_message = f"[{stage}/{total_number_of_stages}] {start_message}..."
        self.finish_message = "done."

    def __enter__(self):
        """Print the start message.

        :return None:
        """
        print(self.start_message, end="", flush=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Print the finish message on the same line as the start message. If there's an error, print "ERROR" instead.

        :return None:
        """
        if exc_type:
            print("ERROR.")
        else:
            print(self.finish_message)
