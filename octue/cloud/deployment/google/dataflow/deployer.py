import pkg_resources

from octue.cloud.deployment.google.base_deployer import BaseDeployer, ProgressMessage
from octue.cloud.deployment.google.dataflow.pipeline import (
    DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    DEFAULT_SETUP_FILE_PATH,
    create_streaming_job,
)


DEFAULT_DATAFLOW_DOCKERFILE_URL = (
    "https://raw.githubusercontent.com/octue/octue-sdk-python/main/octue/cloud/deployment/google/dataflow/Dockerfile"
)

OCTUE_SDK_PYTHON_IMAGE_URI = f"octue/octue-sdk-python:{pkg_resources.get_distribution('octue').version}-slim"


class DataflowDeployer(BaseDeployer):
    """A tool for using an `octue.yaml` file in a repository to build and deploy the repository's `octue` app to Google
    Dataflow as a streaming job. This includes setting up a Google Cloud Build trigger, enabling automatic deployment
    during future development. Note that this tool requires the `gcloud` CLI to be available.

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

    TOTAL_NUMBER_OF_STAGES = 3

    def __init__(self, octue_configuration_path, image_uri_template=None):
        super().__init__(octue_configuration_path, image_uri_template)
        self.build_trigger_description = (
            f"Build the {self.service_configuration.name!r} service and deploy it to Dataflow."
        )

        if not self.service_configuration.temporary_files_location:
            self.service_configuration.temporary_files_location = DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION

        if not self.service_configuration.setup_file_path:
            self.service_configuration.setup_file_path = DEFAULT_SETUP_FILE_PATH

    def deploy(self, no_cache=False, update=False):
        """Create a Google Cloud Build configuration from the `octue.yaml file, create a build trigger, run it, and
        deploy the app as a Google Dataflow streaming job.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return None:
        """
        self._generate_cloud_build_configuration(no_cache=no_cache)
        self._create_build_trigger(update=update)
        self._run_build_trigger()
        print(self.success_message)

    def create_streaming_dataflow_job(self, image_uri, update=False):
        """Deploy the newly-built service as a streaming Dataflow job.

        :param str image_uri: the URI of the image to use in the Dataflow job
        :param bool update: if `True`, update the identically-named existing job
        :return None:
        """
        with ProgressMessage("Deploying streaming Dataflow job", 1, 1) as progress_message:
            if update:
                progress_message.finish_message = "update triggered."

            kwargs = {
                "service_name": self.service_configuration.name,
                "project_name": self.service_configuration.project_name,
                "service_id": self.service_id,
                "region": self.service_configuration.region,
                "setup_file_path": self.service_configuration.setup_file_path,
                "image_uri": image_uri,
                "temporary_files_location": self.service_configuration.temporary_files_location,
                "service_account_email": self.service_configuration.service_account_email,
                "worker_machine_type": self.service_configuration.worker_machine_type,
                "maximum_instances": self.service_configuration.maximum_instances,
                "update": update,
            }

            try:
                create_streaming_job(**kwargs)

            except ValueError as error:
                if not update:
                    raise error

                # If attempting to update a job that doesn't yet exist, set `update` to `False`.
                kwargs["update"] = False
                progress_message.finish_message = "triggered."
                create_streaming_job(**kwargs)

        print(self.success_message)

    def _generate_cloud_build_configuration(self, no_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `generated_cloud_build_configuration` attribute. The configuration steps are:
        1. If no Dockerfile path is provided in `octue.yaml`, get the default Dataflow Dockerfile
        2. Build the image
        3. Push the image to Google Container Registry

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :return None:
        """
        with ProgressMessage(
            "Generating Google Cloud Build configuration",
            1,
            self.TOTAL_NUMBER_OF_STAGES,
        ) as progress_message:

            if self.service_configuration.provided_cloud_build_configuration_path:
                progress_message.finish_message = f"skipped - using {self.service_configuration.provided_cloud_build_configuration_path!r} from repository."
                return

            get_dockerfile_step, dockerfile_path = self._create_get_dockerfile_step(DEFAULT_DATAFLOW_DOCKERFILE_URL)

            if no_cache:
                cache_option = ["--no-cache"]
            else:
                cache_option = []

            required_environment_variable_build_args = [
                f"--build-arg={name}={value}" for name, value in self.required_environment_variables.items()
            ]

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
                                    *required_environment_variable_build_args,
                                    ".",
                                    "'-f'",
                                    dockerfile_path,
                                ]
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
                        "id": "Deploy Dataflow job",
                        "name": OCTUE_SDK_PYTHON_IMAGE_URI,
                        "args": [
                            "octue",
                            "deploy",
                            "dataflow",
                            "--update",
                            "--dataflow-job-only",
                            f"--image-uri={self.image_uri_template}",
                        ],
                    },
                ],
                "images": [self.image_uri_template],
                **available_secrets_option,
            }
