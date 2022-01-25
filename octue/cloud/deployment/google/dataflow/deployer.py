from octue.cloud.deployment.google.base_deployer import BaseDeployer, ProgressMessage
from octue.cloud.deployment.google.dataflow.pipeline import (
    DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION,
    DEFAULT_SETUP_FILE_PATH,
    create_streaming_job,
)


DEFAULT_DATAFLOW_DOCKERFILE_URL = (
    "https://raw.githubusercontent.com/octue/octue-sdk-python/main/octue/cloud/deployment/google/dataflow/Dockerfile"
)

OCTUE_SDK_PYTHON_IMAGE_URI = "eu.gcr.io/octue-amy/octue-sdk-python:0.9.3-slim"


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
    :param str|None service_id: the UUID to give the service if a random one is not suitable
    :return None:
    """

    TOTAL_NUMBER_OF_STAGES = 4

    def __init__(self, octue_configuration_path, service_id=None):
        super().__init__(octue_configuration_path, service_id)
        self.build_trigger_description = f"Build the {self.name!r} service and deploy it to Dataflow."
        self.success_message = f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}."

        # Optional configuration file entries for Dataflow.
        self.temporary_files_location = self._octue_configuration.get(
            "temporary_files_location", DEFAULT_DATAFLOW_TEMPORARY_FILES_LOCATION
        )
        self.setup_file_path = self._octue_configuration.get("setup_file_path", DEFAULT_SETUP_FILE_PATH)

    def deploy(self, no_cache=False, update=False, dataflow_job_only=False):
        """Create a Google Cloud Build configuration from the `octue.yaml file, create a build trigger, run it, and
        deploy the app as a Google Dataflow streaming job.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :param bool dataflow_job_only: if `True`, skip creating and running the build trigger and just deploy a pre-built image to Dataflow
        :return None:
        """
        if dataflow_job_only:
            self._deploy_streaming_dataflow_job(update=update)
            print(self.success_message)
            return

        self._generate_cloud_build_configuration(no_cache=no_cache)
        self._create_build_trigger(update=update)
        self._run_build_trigger()
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
            if self.provided_cloud_build_configuration_path:
                progress_message.finish_message = "skipped - using cloudbuild.yaml file from repository."
                return

            get_dockerfile_step, dockerfile_path = self._create_get_dockerfile_step(DEFAULT_DATAFLOW_DOCKERFILE_URL)

            if no_cache:
                cache_option = ["--no-cache"]
            else:
                cache_option = []

            self.generated_cloud_build_configuration = {
                "steps": [
                    *get_dockerfile_step,
                    {
                        "id": "Build image",
                        "name": "gcr.io/cloud-builders/docker",
                        "args": [
                            "build",
                            *cache_option,
                            *[f"--build-arg={variable}" for variable in self.required_environment_variables],
                            "-t",
                            self.image_uri,
                            ".",
                            "-f",
                            dockerfile_path,
                        ],
                    },
                    {
                        "id": "Push image",
                        "name": "gcr.io/cloud-builders/docker",
                        "args": ["push", self.image_uri],
                    },
                    {
                        "id": "Deploy Dataflow job",
                        "name": OCTUE_SDK_PYTHON_IMAGE_URI,
                        "args": [
                            "octue-app",
                            "deploy",
                            "dataflow",
                            f"--service-id={self.service_id}",
                            "--update",
                            "--dataflow-job-only",
                        ],
                    },
                ],
                "images": [self.image_uri],
            }

    def _deploy_streaming_dataflow_job(self, update=False):
        """Deploy the newly-built service as a streaming Dataflow job.

        :param bool update: if `True`, update the identically-named existing job
        :return None:
        """
        with ProgressMessage("Deploying streaming Dataflow job", 4, self.TOTAL_NUMBER_OF_STAGES) as progress_message:
            if update:
                progress_message.finish_message = "update triggered."

            kwargs = {
                "service_name": self.name,
                "project_name": self.project_name,
                "service_id": self.service_id,
                "region": self.region,
                "setup_file_path": self.setup_file_path,
                "image_uri": self.image_uri,
                "temporary_files_location": self.temporary_files_location,
                "update": update,
            }

            try:
                create_streaming_job(**kwargs)

            except ValueError as error:
                if not update:
                    raise error

                # If attempting to update a job that doesn't yet exist, set `update` to `False`.
                kwargs["update"] = False
                create_streaming_job(**kwargs)
