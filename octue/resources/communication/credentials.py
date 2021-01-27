import json
import os
from tempfile import TemporaryDirectory


class GCPCredentialsManager:
    """ A credentials manager for Google Cloud Platform (GCP) that takes a JSON string from an environment variable and
    writes it to a file in a temporary directory.
    """

    def __init__(self, environment_variable_name="GCP_SERVICE_ACCOUNT"):
        self._temporary_directory = TemporaryDirectory()
        self.path = os.path.join(self._temporary_directory.name, "gcp_credentials.json")
        self.environment_variable_name = environment_variable_name

        with open(self.path, "w") as f:
            json.dump(self._load_credentials_from_environment_variable(), f)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """ Delete the temporary directory containing the credentials. """
        self._temporary_directory.cleanup()

    def _load_credentials_from_environment_variable(self):
        """ Load JSON credentials from environment variable. """
        json_credentials = os.environ.get(self.environment_variable_name)

        if json_credentials is None:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

        return json.loads(json_credentials)
