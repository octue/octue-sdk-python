import tempfile

import yaml

from octue.cloud.deployment.google.base_deployer import BaseDeployer, ProgressMessage
from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
from octue.cloud.pub_sub.subscription import Subscription
from octue.cloud.pub_sub.topic import Topic
from octue.exceptions import DeploymentError
from octue.resources.service_backends import GCPPubSubBackend


DEFAULT_CLOUD_RUN_DOCKERFILE_URL = (
    "https://raw.githubusercontent.com/octue/octue-sdk-python/main/octue/cloud/deployment/google/cloud_run/Dockerfile"
)


class CloudRunDeployer(BaseDeployer):
    """A tool for using an `octue.yaml` file in a repository to build and deploy the repository's `octue` app to Google
    Cloud Run. This includes setting up a Google Cloud Build trigger, enabling automatic deployment during future
    development. Note that this tool requires the `gcloud` CLI to be available.

    Warning: the build triggered by this tool will currently only work if it is run in the correct context directory for
    the docker build - this is due to the `gcloud beta builds triggers run` command not yet working, so the first build
    has to be triggered via `gcloud builds submit`, which collects the context from the local machine running the
    command instead of from the remote repository. This is not ideal and will be addressed as soon as the former
    `gcloud` command works properly.

    The version information for the version of `gcloud` used to develop this tool is:
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
    :param str|None service_id: the UUID to give the service if a random one is not suitable
    :return None:
    """

    TOTAL_NUMBER_OF_STAGES = 5

    def __init__(self, octue_configuration_path, service_id=None):
        super().__init__(octue_configuration_path, service_id)
        self.build_trigger_description = f"Build the {self.name!r} service and deploy it to Cloud Run."

    def deploy(self, no_cache=False, update=False):
        """Create a Google Cloud Build configuration from the `octue.yaml file, create a build trigger, build and
        deploy the app as a Google Cloud Run service, and create and attach an Eventarc Pub/Sub run trigger to it.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger and Eventarc run trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return str: the service's UUID
        """
        self._generate_cloud_build_configuration(no_cache=no_cache)

        # Put the Cloud Build configuration into a temporary file so it can be used by the `gcloud` commands.
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "w") as f:
                yaml.dump(self.cloud_build_configuration, f)

            self._create_build_trigger(cloud_build_configuration_path=temporary_file.name, update=update)
            self._run_build_trigger(cloud_build_configuration_path=temporary_file.name)

        self._allow_unauthenticated_messages()
        self._create_eventarc_run_trigger(update=update)

        print(f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}.")
        return self.service_id

    def _generate_cloud_build_configuration(self, no_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `cloud_build_configuration` attribute. The configuration steps are:
        1. If no Dockerfile path is provided in `octue.yaml`, getting the default Cloud Run Dockerfile
        2. Building the image
        3. Pushing the image to Google Container Registry
        4. Deploying the image to Cloud Run

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :return None:
        """
        with ProgressMessage("Generating Google Cloud Build configuration", 1, self.TOTAL_NUMBER_OF_STAGES):
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
                        "args": ["wget", DEFAULT_CLOUD_RUN_DOCKERFILE_URL],
                    }
                ]

                dockerfile_path = "Dockerfile"

            if no_cache:
                cache_option = ["--no-cache"]
            else:
                cache_option = []

            environment_variables = ",".join(
                [f"{variable['name']}={variable['value']}" for variable in self.environment_variables]
                + self.required_environment_variables
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

    def _allow_unauthenticated_messages(self):
        """Allow unauthenticated messages on the service, allowing the service to be reached via Pub/Sub. Note that
        permissions are still required to send messages via Pub/Sub.

        :return None:
        """
        with ProgressMessage("Making service available via Pub/Sub", 4, self.TOTAL_NUMBER_OF_STAGES):
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

    def _create_eventarc_run_trigger(self, update=False):
        """Create an Eventarc run trigger for the service. Update the Eventarc subscription to have the minimum
        acknowledgement deadline to avoid recurrent re-computation of questions.

        :raise octue.exceptions.DeploymentError: if the Eventarc subscription is not found after creating the Eventarc trigger
        :param bool update: if `True`, ignore "already exists" errors from the Eventarc trigger
        :return None:
        """
        with ProgressMessage(
            "Creating Eventarc Pub/Sub run trigger",
            5,
            self.TOTAL_NUMBER_OF_STAGES,
        ) as progress_message:
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

            try:
                self._run_command(command)

                eventarc_subscription_path = None

                for subscription_path in topic.get_subscriptions():
                    if self.name in subscription_path:
                        eventarc_subscription_path = subscription_path
                        break

                if not eventarc_subscription_path:
                    raise DeploymentError(
                        "Eventarc subscription not found - it may exist but its acknowledgement has not been updated to "
                        "the minimum value, which may lead to recurrent re-computation of questions."
                    )

                # Set the acknowledgement deadline to the minimum value to avoid recurrent re-computation of questions.
                subscription = Subscription(
                    name=eventarc_subscription_path.split("/")[-1],
                    topic=topic,
                    project_name=self.project_name,
                    ack_deadline=10,
                )

                subscription.update()

            except DeploymentError as e:
                self._raise_or_ignore_already_exists_error(e, update, progress_message)