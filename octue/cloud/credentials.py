import json
import logging
import os
import warnings
from google.oauth2 import service_account


logger = logging.getLogger(__name__)


class GCPCredentialsManager:
    """A credentials manager for Google Cloud Platform (GCP) that takes a path to a service account JSON file, or a
    JSON string of the contents of such a service account file, from the given environment variable and instantiates
    a Google Cloud credentials object.

    :param str environment_variable_name:
    :return None:
    """

    def __init__(self, environment_variable_name="GOOGLE_APPLICATION_CREDENTIALS"):
        self.environment_variable_name = environment_variable_name

        if self.environment_variable_name is None:
            self.service_account_json = None
            return

        try:
            self.service_account_json = os.environ[self.environment_variable_name]
        except KeyError:
            warnings.warn(
                f"No environment variable called {self.environment_variable_name!r}; resorting to default Google Cloud "
                f"credentials."
            )
            self.service_account_json = None

    def get_credentials(self):
        """Get the Google OAUTH2 service account credentials.

        :return google.auth.service_account.Credentials:
        """
        if self.service_account_json is None:
            return None

        # Check that the environment variable refers to a real file.
        if os.path.exists(self.service_account_json):
            return self._get_credentials_from_file()

        # If it doesn't, assume that it's the credentials file as a JSON string.
        return self._get_credentials_from_string()

    def _get_credentials_from_file(self):
        with open(self.service_account_json) as f:
            credentials = json.load(f)

        logger.debug("GCP credentials read from file.")
        return service_account.Credentials.from_service_account_info(credentials)

    def _get_credentials_from_string(self):
        credentials = json.loads(self.service_account_json)
        logger.debug("GCP credentials loaded from string.")
        return service_account.Credentials.from_service_account_info(credentials)
