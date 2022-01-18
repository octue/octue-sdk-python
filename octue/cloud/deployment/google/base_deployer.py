import subprocess
import uuid

import yaml

from octue.exceptions import DeploymentError


DOCKER_REGISTRY_URL = "eu.gcr.io"


class BaseDeployer:
    TOTAL_NUMBER_OF_STAGES = None

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
        self.service_id = service_id or str(uuid.uuid4())
        self.build_trigger_description = f"Build the {self.name!r} service and deploy it to Cloud Run."
        self.required_environment_variables = [f"SERVICE_ID={self.service_id}", f"SERVICE_NAME={self.name}"]

        self._default_image_uri = (
            f"{DOCKER_REGISTRY_URL}/{self.project_name}/{self.repository_owner}/{self.repository_name}/"
            f"{self.name}:{self._get_short_head_commit_hash()}"
        )

        # Optional configuration file entries.
        self.dockerfile_path = octue_configuration.get("dockerfile_path")
        self.minimum_instances = octue_configuration.get("minimum_instances", 0)
        self.maximum_instances = octue_configuration.get("maximum_instances", 10)
        self.concurrency = octue_configuration.get("concurrency", 10)
        self.image_uri = octue_configuration.get("image_uri", self._default_image_uri)
        self.branch_pattern = octue_configuration.get("branch_pattern", "^main$")
        self.memory = octue_configuration.get("memory", "128Mi")
        self.cpus = octue_configuration.get("cpus", 1)
        self.environment_variables = octue_configuration.get("environment_variables", [])

    def _create_build_trigger(self, cloud_build_configuration_path, update=False):
        """Create the build trigger in Google Cloud Build using the given `cloudbuild.yaml` file.

        :param str cloud_build_configuration_path: the path to the `cloudbuild.yaml` file (it can have a different name or extension but it needs to be in the `cloudbuild.yaml` format)
        :param bool update: if `True` and there is an existing trigger, delete it and replace it with an updated one
        :return None:
        """
        with ProgressMessage("Creating build trigger", 2, self.TOTAL_NUMBER_OF_STAGES) as progress_message:
            create_trigger_command = [
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

            try:
                self._run_command(create_trigger_command)
            except DeploymentError as e:
                self._raise_or_ignore_already_exists_error(e, update, progress_message, finish_message="recreated.")

                delete_trigger_command = [
                    "gcloud",
                    "beta",
                    "builds",
                    "triggers",
                    "delete",
                    f"{self.name}",
                ]

                self._run_command(delete_trigger_command)
                self._run_command(create_trigger_command)

    def _build_and_deploy_service(self, cloud_build_configuration_path):
        """Build and deploy the service from the given Cloud Build configuration file and local context. This method
        must be run from the same directory that `docker build -f <path/to/Dockerfile .` would be run from locally for
        the correct build context to be available. When `gcloud beta builds triggers run` is working, this won't be
        necessary as the build context can just be taken from the relevant GitHub repository.

        :param str cloud_build_configuration_path: the path to the `cloudbuild.yaml` file (it can have a different name or extension but it needs to be in the `cloudbuild.yaml` format)
        :return None:
        """
        with ProgressMessage("Building and deploying service", 3, self.TOTAL_NUMBER_OF_STAGES):
            build_command = [
                "gcloud",
                "builds",
                "submit",
                ".",
                f"--config={cloud_build_configuration_path}",
            ]

            self._run_command(build_command)

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
        :return None:
        """
        process = subprocess.run(command, capture_output=True)

        if process.returncode != 0:
            raise DeploymentError(process.stderr.decode())

    @staticmethod
    def _raise_or_ignore_already_exists_error(exception, update, progress_message, finish_message=None):
        """If `update` is `True` and the exception includes the words "already exists", ignore the exception and change
        the progress message's `finish_message` to "already exists."; otherwise, raise the exception.

        :param Exception exception: the exception to ignore or raise
        :param bool update: if `True`, ignore "already exists" errors but raise other errors
        :param ProgressMessage progress_message: the progress message to update with "already exists" if appropriate
        :param str finish_message:
        :raise Exception:
        :return None:
        """
        if update and "already exists" in exception.args[0]:
            progress_message.finish_message = finish_message or "already exists."
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
