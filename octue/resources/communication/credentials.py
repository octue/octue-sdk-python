import json
import logging
import os
from google.oauth2 import service_account


logger = logging.getLogger(__name__)


class GCPCredentialsManager:
    """ A credentials manager for Google Cloud Platform (GCP) that takes a JSON string from an environment variable and
    writes it to a file in a temporary directory.
    """

    def __init__(self, environment_variable_name="GCP_SERVICE_ACCOUNT"):
        self.environment_variable_name = environment_variable_name

    def get_credentials(self):
        try:
            environment_variable_value = os.environ[self.environment_variable_name]
        except KeyError:
            raise EnvironmentError(f"There is no environment variable called {self.environment_variable_name}.")

        # Check that the environment variable refers to a valid *and* real path.
        if os.path.exists(environment_variable_value):
            return self._get_credentials_from_file(environment_variable_value)

        # If it doesn't, assume that it's the credentials file as a JSON string.
        return self._get_credentials_from_string(environment_variable_value)

    def _get_credentials_from_file(self, environment_variable_value):
        with open(environment_variable_value) as f:
            credentials = json.load(f)

        logger.debug("GCP credentials read from file.")
        return service_account.Credentials.from_service_account_info(credentials)

    def _get_credentials_from_string(self, environment_variable_value):
        credentials = json.loads(environment_variable_value)
        logger.debug("GCP credentials loaded from string.")
        return service_account.Credentials.from_service_account_info(credentials)
