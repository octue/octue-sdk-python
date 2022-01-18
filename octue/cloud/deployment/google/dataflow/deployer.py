import tempfile

import yaml

from octue.cloud.deployment.google.base_deployer import BaseDeployer, ProgressMessage
from octue.cloud.deployment.google.dataflow.deploy import deploy_streaming_pipeline


DEFAULT_DATAFLOW_DOCKERFILE_URL = (
    "https://raw.githubusercontent.com/octue/octue-sdk-python/main/octue/cloud/deployment/google/dataflow/Dockerfile"
)


class DataflowDeployer(BaseDeployer):

    TOTAL_NUMBER_OF_STAGES = 4

    def __init__(self, octue_configuration_path, service_id=None):
        super().__init__(octue_configuration_path, service_id)
        self.build_trigger_description = f"Build the {self.name!r} service and deploy it to Dataflow."

    def deploy(self, no_cache=False, no_build=False, update=False):
        """Create a Google Cloud Build configuration from the `octue.yaml file, create a build trigger, run it, and
        deploy the app as a Google Dataflow streaming job.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return str: the service's UUID
        """
        self._generate_cloud_build_configuration(no_cache=no_cache)

        if not no_build:
            # Put the Cloud Build configuration into a temporary file so it can be used by the `gcloud` commands.
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                with open(temporary_file.name, "w") as f:
                    yaml.dump(self.cloud_build_configuration, f)

                self._create_build_trigger(cloud_build_configuration_path=temporary_file.name, update=update)
                self._run_build_trigger(cloud_build_configuration_path=temporary_file.name)

        self._deploy_streaming_dataflow_job(update=update)

        print(f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}.")
        return self.service_id

    def _generate_cloud_build_configuration(self, no_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `cloud_build_configuration` attribute. The configuration steps are:
        1. If no Dockerfile path is provided in `octue.yaml`, getting the default Dataflow Dockerfile
        2. Building the image
        3. Pushing the image to Google Container Registry

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :return None:
        """
        with ProgressMessage("Generating Google Cloud Build configuration", 1, self.TOTAL_NUMBER_OF_STAGES):
            if self.dockerfile_path:
                get_dockerfile_step = []
                dockerfile_path = self.dockerfile_path

            else:
                # If no path to a dockerfile has been provided, add a step to download the default Octue service
                # Dockerfile to build the image from.
                get_dockerfile_step = [
                    {
                        "id": "Get default Octue Dockerfile",
                        "name": "alpine:latest",
                        "args": ["wget", DEFAULT_DATAFLOW_DOCKERFILE_URL],
                    }
                ]

                dockerfile_path = "Dockerfile"

            if no_cache:
                cache_option = ["--no-cache"]
            else:
                cache_option = []

            self.cloud_build_configuration = {
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
                ],
                "images": [self.image_uri],
            }

    def _deploy_streaming_dataflow_job(self, update=False):
        """Deploy the newly-built service as a Dataflow streaming job.

        :param bool update: if `True`, update the existing job with the same name
        :return None:
        """
        with ProgressMessage("Deploying streaming Dataflow job", 4, self.TOTAL_NUMBER_OF_STAGES) as progress_message:
            if update:
                progress_message.finish_message = "updated."

            deploy_streaming_pipeline(
                service_name=self.name,
                project_name=self.project_name,
                service_id=self.service_id,
                region=self.region,
                image_uri=self.image_uri,
                update=update,
            )
