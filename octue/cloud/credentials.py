import json
import logging
import os
import warnings
from google.auth import compute_engine
from google.oauth2 import service_account


logger = logging.getLogger(__name__)


class GCPCredentialsManager:
    """A credentials manager for Google Cloud Platform (GCP) that takes a path to a service account JSON file, or a
    JSON string of the contents of such a service account file, from the given environment variable and instantiates
    a Google Cloud credentials object.

    :param str|None environment_variable_name:
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
            logger.debug(
                "No environment variable called %r; resorting to default Google Cloud credentials.",
                self.environment_variable_name,
            )
            self.service_account_json = None

    def get_credentials(self, as_dict=False):
        """Get the Google OAUTH2 service account credentials.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials|None:
        """
        if as_dict:
            warnings.warn(
                message=(
                    "Requesting credentials as a dictionary is deprecated"
                    "to enable uniform treatment between specified"
                    "GOOGLE_APPLICATION_CREDENTIALS and use of Google's"
                    "Application Default Credentials."
                ),
                category=DeprecationWarning,
            )

        if self.service_account_json is None:
            logger.debug("Using application default credentials")
            creds = compute_engine.Credentials()

        elif os.path.exists(self.service_account_json):
            logger.debug("Using credentials from file %s", self.service_account_json)
            creds = self._get_credentials_from_file(as_dict=as_dict)

        else:
            warnings.warn(
                message=(
                    "Placing service account credentials JSON directly in an"
                    "environment variable will be deprecated soon. Set "
                    "GOOGLE_APPLICATION_CREDENTIALS to a file path, or leave"
                    "unset to use Application Default Credentials."
                    "See https://medium.com/datamindedbe/application-default-credentials-477879e31cb5"
                    "for a helpful overview of google credentials"
                ),
                category=DeprecationWarning,
            )

            logger.debug("Using credentials from environment string")
            creds = self._get_credentials_from_string(as_dict=as_dict)

        return creds

    def _get_credentials_from_file(self, as_dict=False):
        """Get the credentials from the JSON file whose path is specified in the environment variable's value.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials:
        """
        with open(self.service_account_json) as f:
            credentials = json.load(f)

        logger.debug("GCP credentials read from file.")

        if as_dict:
            return credentials

        return service_account.Credentials.from_service_account_info(credentials)

    def _get_credentials_from_string(self, as_dict=False):
        """Get the credentials directly from the JSON string specified in the environment variable's value.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials:
        """
        credentials = json.loads(self.service_account_json)
        logger.debug("GCP credentials loaded from string.")

        if as_dict:
            return credentials

        return service_account.Credentials.from_service_account_info(credentials)
