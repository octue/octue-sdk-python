import json
import logging
import os
from google.oauth2 import service_account


logger = logging.getLogger(__name__)


class GCPCredentialsManager:
    """A credentials manager for Google Cloud Platform (GCP) that takes a JSON string from an environment variable and
    writes it to a file in a temporary directory.
    """

    def __init__(self, environment_variable_name="GOOGLE_APPLICATION_CREDENTIALS"):
        self.environment_variable_name = environment_variable_name

        try:
            self.environment_variable_value = os.environ[self.environment_variable_name]
        except KeyError:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

    def get_credentials(self):
        """Get the Google OAUTH2 service account credentials for which the environment variable value is either the
        filename containing them or a JSON string version of them."""

        # Check that the environment variable refers to a valid *and* real path.
        if os.path.exists(self.environment_variable_value):
            return self._get_credentials_from_file()

        # If it doesn't, assume that it's the credentials file as a JSON string.
        return self._get_credentials_from_string()

    def _get_credentials_from_file(self):
        with open(self.environment_variable_value) as f:
            credentials = json.load(f)

        logger.debug("GCP credentials read from file.")
        return service_account.Credentials.from_service_account_info(credentials)

    def _get_credentials_from_string(self):
        credentials = json.loads(self.environment_variable_value)
        logger.debug("GCP credentials loaded from string.")
        return service_account.Credentials.from_service_account_info(credentials)
