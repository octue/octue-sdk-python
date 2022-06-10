import json
import subprocess
import tempfile
import time
from abc import abstractmethod

import yaml

from octue.configuration import ServiceConfiguration
from octue.exceptions import DeploymentError


DOCKER_REGISTRY_URL = "eu.gcr.io"


class BaseDeployer:
    """An abstract tool for using an `octue.yaml` file in a repository to build and deploy the repository's `octue` app.
    This includes setting up a Google Cloud Build trigger, enabling automatic deployment during future development.
    Note that this tool requires the `gcloud` CLI to be available.

    This tool can be inherited from to create specific deployment tools (e.g. for Google Cloud Run or Google Dataflow)
    with the `deploy` and `_generate_cloud_build_configuration` methods overridden to set where and how the app is
    deployed. The `TOTAL_NUMBER_OF_STAGES` class variable should also be overridden and set to the number of stages in
    the subclass's deployment.

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

    TOTAL_NUMBER_OF_STAGES = None

    def __init__(self, octue_configuration_path, image_uri_template=None):
        self.service_configuration = ServiceConfiguration.from_file(octue_configuration_path)

        # Generated attributes.
        self.build_trigger_description = None
        self.generated_cloud_build_configuration = None
        self.service_id = self.service_configuration.service_id.replace("/", ".")

        self.required_environment_variables = {"SERVICE_NAME": self.service_configuration.name}

        self.image_uri_template = image_uri_template or (
            f"{DOCKER_REGISTRY_URL}/{self.service_configuration.project_name}/"
            f"{self.service_configuration.repository_name}/{self.service_configuration.name}:$SHORT_SHA"
        )

        self.success_message = f"[SUCCESS] Service deployed - it can be questioned via Pub/Sub at {self.service_id!r}."

    @abstractmethod
    def deploy(self, no_cache=False, update=False):
        """Deploy the octue app.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :param bool update: if `True`, allow the build trigger to already exist and just build and deploy a new image based on an updated `octue.yaml` file
        :return str: the service's UUID
        """

    @abstractmethod
    def _generate_cloud_build_configuration(self, no_cache=False):
        """Generate a Google Cloud Build configuration equivalent to a `cloudbuild.yaml` file in memory and assign it
        to the `generated_cloud_build_configuration` attribute.

        :param bool no_cache: if `True`, don't use the Docker cache when building the image
        :return None:
        """

    def _create_get_dockerfile_step(self, default_dockerfile_url):
        """If a Dockerfile step hasn't been provided, create a `cloudbuild.yaml` step that downloads the relevant
        default `octue` Dockerfile to build the image from. If it has been provided, provide an empty step.

        :param str default_dockerfile_url: the URL to get the default `octue` Dockerfile from
        :return (list, str): the `cloudbuild.yaml` step and the path to the Dockerfile
        """
        if self.service_configuration.dockerfile_path:
            return [], self.service_configuration.dockerfile_path

        # If no path to a dockerfile has been provided, add a step to download the default Octue service
        # Dockerfile to build the image from.
        get_dockerfile_step = [
            {
                "id": "Get default Octue Dockerfile",
                "name": "alpine:latest",
                "args": ["wget", default_dockerfile_url],
            }
        ]

        return get_dockerfile_step, "Dockerfile"

    def _create_build_secrets_sections(self):
        """Create the build secrets options for the Cloud Build configuration so secrets from the Google Cloud Secrets
        Manager can be used during the build step of the Cloud Build process. This provides the `availableSecrets`
        section for the overall configuration as well as Docker build args and the `secretEnv` section for the build
        step.

        :return (dict, dict): the `availableSecrets` section and the build secrets, the latter containing the Docker build args and `secretEnv` section for the Cloud Build build step
        """
        available_secrets_option = {}
        build_secrets = {"build_args": "", "secret_env": {}}

        if not self.service_configuration.secrets.get("build"):
            return available_secrets_option, build_secrets

        available_secrets_option = {
            "availableSecrets": {
                "secretManager": [
                    {
                        "versionName": f"projects/{self.service_configuration.project_name}/secrets/{secret_name}/versions/latest",
                        "env": secret_name,
                    }
                    for secret_name in self.service_configuration.secrets["build"]
                ]
            }
        }

        build_secrets["secret_env"] = {"secretEnv": self.service_configuration.secrets["build"]}

        build_secrets["build_args"] = [
            f"--build-arg={secret_name}=$${secret_name}" for secret_name in self.service_configuration.secrets["build"]
        ]

        return available_secrets_option, build_secrets

    def _create_build_trigger(self, update=False):
        """Create the build trigger in Google Cloud Build using the given `cloudbuild.yaml` file.

        :param bool update: if `True` and there is an existing trigger, delete it and replace it with an updated one
        :return None:
        """
        with ProgressMessage("Creating build trigger", 2, self.TOTAL_NUMBER_OF_STAGES) as progress_message:

            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:

                if self.service_configuration.provided_cloud_build_configuration_path:
                    configuration_option = [
                        f"--build-config={self.service_configuration.provided_cloud_build_configuration_path}"
                    ]

                else:
                    # Put the Cloud Build configuration into a temporary file so it can be used by the `gcloud` command.
                    with open(temporary_file.name, "w") as f:
                        yaml.dump(self.generated_cloud_build_configuration, f)

                    configuration_option = [f"--inline-config={temporary_file.name}"]

                create_trigger_command = [
                    "gcloud",
                    f"--project={self.service_configuration.project_name}",
                    "beta",
                    "builds",
                    "triggers",
                    "create",
                    "github",
                    f"--name={self.service_configuration.name}",
                    f"--repo-name={self.service_configuration.repository_name}",
                    f"--repo-owner={self.service_configuration.repository_owner}",
                    f"--description={self.build_trigger_description}",
                    f"--branch-pattern={self.service_configuration.branch_pattern}",
                    *configuration_option,
                ]

                try:
                    self._run_command(create_trigger_command)
                except DeploymentError as e:
                    self._raise_or_ignore_already_exists_error(e, update, progress_message, finish_message="recreated.")

                    delete_trigger_command = [
                        "gcloud",
                        f"--project={self.service_configuration.project_name}",
                        "beta",
                        "builds",
                        "triggers",
                        "delete",
                        f"{self.service_configuration.name}",
                    ]

                    self._run_command(delete_trigger_command)
                    self._run_command(create_trigger_command)

    def _run_build_trigger(self):
        """Run the build trigger and return the build ID. The image URI is updated from the build metadata, ensuring
        that, if a `cloudbuild.yaml` file is provided instead of generated, the correct image URI from this file is
        used in later steps.

        :return None:
        """
        with ProgressMessage(
            f"Running build trigger on branch {self.service_configuration.branch_pattern!r}",
            3,
            self.TOTAL_NUMBER_OF_STAGES,
        ):

            build_command = [
                "gcloud",
                f"--project={self.service_configuration.project_name}",
                "--format=json",
                "beta",
                "builds",
                "triggers",
                "run",
                self.service_configuration.name,
                f"--branch={self.service_configuration.branch_pattern.strip('^$')}",
            ]

            process = self._run_command(build_command)
            metadata = json.loads(process.stdout.decode())["metadata"]
            self._wait_for_build_to_finish(metadata["build"]["id"])

    def _wait_for_build_to_finish(self, build_id, check_period=20):
        """Wait for the build with the given ID to finish.

        :param str build_id: the ID of the build to wait for
        :param float check_period: the period in seconds at which to check if the build has finished
        :return None:
        """
        get_build_command = [
            "gcloud",
            f"--project={self.service_configuration.project_name}",
            "--format=json",
            "builds",
            "describe",
            build_id,
        ]

        while True:
            process = self._run_command(get_build_command)
            status = json.loads(process.stdout.decode())["status"]

            if status not in {"QUEUED", "WORKING", "SUCCESS"}:
                raise DeploymentError(f"The build status is {status!r}.")

            if status == "SUCCESS":
                break

            time.sleep(check_period)

    @staticmethod
    def _get_short_head_commit_hash():
        """Get the short commit hash for the HEAD commit in the current git repository.

        :return str:
        """
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True).stdout.decode().strip()

    @staticmethod
    def _run_command(command):
        """Run a command in a subprocess, raising a `DeploymentError` if it fails.

        :param iter(str) command: the command to run in `subprocess` form e.g. `["cat", "my_file.txt"]`
        :raise octue.exceptions.DeploymentError: if the command fails
        :return subprocess.CompletedProcess:
        """
        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise DeploymentError(process.stderr.decode())

        return process

    @staticmethod
    def _raise_or_ignore_already_exists_error(exception, update, progress_message, finish_message="already exists."):
        """If `update` is `True` and the exception includes the words "already exists", ignore the exception and update
        the progress message's `finish_message`; otherwise, raise the exception.

        :param Exception exception: the exception to ignore or raise
        :param bool update: if `True`, ignore "already exists" errors but raise other errors
        :param ProgressMessage progress_message: the progress message to update with the finish message if appropriate
        :param str finish_message:
        :raise Exception:
        :return None:
        """
        if update and "already exists" in exception.args[0]:
            progress_message.finish_message = finish_message
            return

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

        :return ProgressMessage:
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
