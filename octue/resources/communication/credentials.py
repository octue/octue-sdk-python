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

    def set_credentials_path_from_path_in_environment_variable(self):
        """ Set the credentials path from the path in the environment variable. """
        try:
            self.path = os.environ[self.environment_variable_name]
        except KeyError:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

        logger.debug("GCP credentials accessed from file.")

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "_temporary_directory"):
            self.cleanup()

    def save_credentials_string_to_temporary_file(self):
        """ Save credentials string in the environment variable to a JSON temporary file. """
        self._temporary_directory = TemporaryDirectory()
        self.path = os.path.join(self._temporary_directory.name, "gcp_credentials.json")

        with open(self.path, "w") as f:
            json.dump(self._load_credentials_from_environment_variable(), f)

        logger.debug("GCP credentials saved to temporary file prior to accessing.")

    def cleanup(self):
        """ Delete the temporary directory containing the credentials. """
        self._temporary_directory.cleanup()

    def _load_credentials_from_environment_variable(self):
        """ Load JSON credentials from environment variable. """
        json_credentials = os.environ.get(self.environment_variable_name)

        if json_credentials is None:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

        return json.loads(json_credentials)
