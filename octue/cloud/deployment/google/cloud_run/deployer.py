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
    :return None:
    """

    TOTAL_NUMBER_OF_STAGES = 5

    def __init__(self, octue_configuration_path, image_uri_template=None):
        super().__init__(octue_configuration_path, image_uri_template)
        self.build_trigger_description = (
            f"Build the {self.service_configuration.name!r} service and deploy it to Cloud Run."
        )

    def deploy(self, no_cache=False, update=False):
        """Create a Google Cloud Build configuration from the `octue.yaml` file, create a build trigger, and run the
        trigger. This deploys the app as a Google Cloud Run service and creates an Eventarc Pub/Sub run trigger for it.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger and Eventarc run trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return str: the service's UUID
        """
        self._generate_cloud_build_configuration(no_cache=no_cache)
        self._create_build_trigger(update=update)
        self._run_build_trigger()
        self._allow_unauthenticated_messages()
        self._create_eventarc_run_trigger(update=update)

        print(self.success_message)
        return self.service_id

    def _generate_cloud_build_configuration(self, no_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `cloud_build_configuration` attribute. The configuration steps are:
        1. If no Dockerfile path is provided in `octue.yaml`, get the default `octue` Cloud Run Dockerfile
        2. Build the image
        3. Push the image to Google Container Registry
        4. Deploy the image to Cloud Run

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :return None:
        """
        with ProgressMessage(
            "Generating Google Cloud Build configuration",
            1,
            self.TOTAL_NUMBER_OF_STAGES,
        ) as progress_message:

            if self.service_configuration.provided_cloud_build_configuration_path:
                progress_message.finish_message = (
                    f"skipped - using {self.service_configuration.provided_cloud_build_configuration_path!r} from "
                    "repository."
                )
                return

            get_dockerfile_step, dockerfile_path = self._create_get_dockerfile_step(DEFAULT_CLOUD_RUN_DOCKERFILE_URL)

            if no_cache:
                cache_option = ["--no-cache"]
            else:
                cache_option = []

            environment_variables = ",".join(
                [
                    f"{variable['name']}={variable['value']}"
                    for variable in self.service_configuration.environment_variables
                ]
                + [f"{name}={value}" for name, value in self.required_environment_variables.items()]
            )

            available_secrets_option, build_secrets = self._create_build_secrets_sections()

            self.generated_cloud_build_configuration = {
                "steps": [
                    *get_dockerfile_step,
                    {
                        "id": "Build image",
                        "name": "gcr.io/cloud-builders/docker",
                        "entrypoint": "bash",
                        "args": [
                            "-c",
                            " ".join(
                                [
                                    "docker",
                                    "build",
                                    *cache_option,
                                    "'-t'",
                                    f"{self.image_uri_template!r}",
                                    *build_secrets["build_args"],
                                    ".",
                                    "'-f'",
                                    dockerfile_path,
                                ],
                            ),
                        ],
                        **build_secrets["secret_env"],
                    },
                    {
                        "id": "Push image",
                        "name": "gcr.io/cloud-builders/docker",
                        "args": ["push", self.image_uri_template],
                    },
                    {
                        "id": "Deploy image to Google Cloud Run",
                        "name": "gcr.io/google.com/cloudsdktool/cloud-sdk:slim",
                        "entrypoint": "gcloud",
                        "args": [
                            "run",
                            "services",
                            "update",
                            self.service_configuration.name,
                            "--platform=managed",
                            f"--image={self.image_uri_template}",
                            f"--region={self.service_configuration.region}",
                            f"--memory={self.service_configuration.memory}",
                            f"--cpu={self.service_configuration.cpus}",
                            f"--set-env-vars={environment_variables}",
                            "--timeout=3600",
                            f"--concurrency={self.service_configuration.concurrency}",
                            f"--min-instances={self.service_configuration.minimum_instances}",
                            f"--max-instances={self.service_configuration.maximum_instances}",
                            "--ingress=internal",
                        ],
                    },
                ],
                "images": [self.image_uri_template],
                **available_secrets_option,
            }

    def _allow_unauthenticated_messages(self):
        """Allow unauthenticated messages on the service, allowing the service to be reached via Pub/Sub. Note that
        permissions are still required to send messages via Pub/Sub.

        :return None:
        """
        with ProgressMessage("Making service available via Pub/Sub", 4, self.TOTAL_NUMBER_OF_STAGES):
            allow_unauthenticated_messages_command = [
                "gcloud",
                f"--project={self.service_configuration.project_name}",
                "run",
                "services",
                "add-iam-policy-binding",
                self.service_configuration.name,
                f"--region={self.service_configuration.region}",
                "--member=allUsers",
                "--role=roles/run.invoker",
            ]

            self._run_command(allow_unauthenticated_messages_command)

    def _create_eventarc_run_trigger(self, update=False):
        """Create a topic and Eventarc run trigger for the service, updating the Eventarc subscription to have the
        minimum acknowledgement deadline to avoid recurrent re-computation of questions.

        :raise octue.exceptions.DeploymentError: if the Eventarc subscription is not found after creating the Eventarc trigger
        :param bool update: if `True`, ignore "already exists" errors from the Eventarc trigger
        :return None:
        """
        with ProgressMessage(
            "Creating Eventarc Pub/Sub run trigger",
            5,
            self.TOTAL_NUMBER_OF_STAGES,
        ) as progress_message:
            service = Service(
                backend=GCPPubSubBackend(project_name=self.service_configuration.project_name),
                service_id=self.service_id,
            )
            topic = Topic(name=self.service_id, namespace=OCTUE_NAMESPACE, service=service)
            topic.create(allow_existing=True)

            command = [
                "gcloud",
                f"--project={self.service_configuration.project_name}",
                "beta",
                "eventarc",
                "triggers",
                "create",
                f"{self.service_configuration.name}-trigger",
                "--matching-criteria=type=google.cloud.pubsub.topic.v1.messagePublished",
                f"--destination-run-service={self.service_configuration.name}",
                f"--location={self.service_configuration.region}",
                f"--transport-topic={topic.name}",
            ]

            try:
                self._run_command(command)

                eventarc_subscription_path = None

                for subscription_path in topic.get_subscriptions():
                    if self.service_configuration.name in subscription_path:
                        eventarc_subscription_path = subscription_path
                        break

                if not eventarc_subscription_path:
                    raise DeploymentError(
                        "Eventarc subscription not found - it may exist but its acknowledgement deadline has not been "
                        "updated to the minimum value, which may lead to recurrent re-computation of questions."
                    )

                # Set the acknowledgement deadline to the minimum value to avoid recurrent re-computation of questions.
                subscription = Subscription(
                    name=eventarc_subscription_path.split("/")[-1],
                    topic=topic,
                    project_name=self.service_configuration.project_name,
                )

                subscription.update()

            except DeploymentError as e:
                self._raise_or_ignore_already_exists_error(e, update, progress_message)
