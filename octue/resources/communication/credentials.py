import json
import logging
import os
from tempfile import TemporaryDirectory


logger = logging.getLogger(__name__)


class GCPCredentialsManager:
    """ A credentials manager for Google Cloud Platform (GCP) that takes a JSON string from an environment variable and
    writes it to a file in a temporary directory.
    """

    def __init__(self, environment_variable_name="GCP_SERVICE_ACCOUNT"):
        self.environment_variable_name = environment_variable_name
        self._temporary_directory = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._temporary_directory is not None:
            self.cleanup()

    def get_credentials_path(self):
        try:
            environment_variable_value = os.environ[self.environment_variable_name]
        except KeyError:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

        # Check that the environment variable refers to a valid *and* real path.
        if os.path.exists(environment_variable_value):
            logger.debug("GCP credentials file found.")
            return environment_variable_value

        # If it doesn't, assume that it's the credentials file as a JSON string.
        return self._read_json_string_and_save_to_temporary_json_file(environment_variable_value)

    def _read_json_string_and_save_to_temporary_json_file(self, environment_variable_value):
        """ Save credentials string in the environment variable to a temporary JSON file. """
        self._temporary_directory = TemporaryDirectory()
        path = os.path.join(self._temporary_directory.name, "gcp_credentials.json")

        with open(path, "w") as f:
            json.dump(json.loads(environment_variable_value), f)

        logger.debug("GCP credentials saved to temporary file.")
        return path

    def cleanup(self):
        """ Delete the temporary directory containing the credentials. """
        self._temporary_directory.cleanup()
