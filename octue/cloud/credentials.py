import json
import logging
import os
import warnings
from json.decoder import JSONDecodeError
from google.auth import compute_engine
from google.oauth2 import service_account
from octue.exceptions import InvalidInputException

logger = logging.getLogger(__name__)

DEFAULT_ENVIRONMENT_VARIABLE_NAME = "GOOGLE_APPLICATION_CREDENTIALS"


class GCPCredentialsManager:
    """A credentials manager for Google Cloud Platform (GCP) that takes a path to a service account JSON file, or a
    JSON string of the contents of such a service account file, from the given environment variable and instantiates
    a Google Cloud credentials object.

    :param str environment_variable_name:
    :return None:
    """

    def __init__(self, environment_variable_name=DEFAULT_ENVIRONMENT_VARIABLE_NAME):

        if environment_variable_name is None:
            raise InvalidInputException(
                "Cannot accept `None` as an environment variable name. Either specify the name as a string or use the default value"
            )

        self.environment_variable_name = environment_variable_name

        # Note: We use '' rather than None to allow us to patch the value in testing (you cannot correctly unset variables in a patch of os.environ)
        self.environment_variable_value = os.getenv(environment_variable_name, "")

        if self.environment_variable_value == "":
            if self.environment_variable_name != DEFAULT_ENVIRONMENT_VARIABLE_NAME:
                raise InvalidInputException(
                    f"Specified use of credentials environment variable {environment_variable_name} but its value is unset"
                )

        logger.debug("Initialised CredentialsManager with %s", environment_variable_name)

    @property
    def using_application_default_credentials(self):
        """A boolean determining whether ADCs are used or not"""
        return self.environment_variable_value == ""

    def get_credentials(self, as_dict=False):
        """Get the Google OAUTH2 service account credentials.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials|None:
        """
        if as_dict:
            warnings.warn(
                message=(
                    "Requesting credentials as a dictionary is deprecated "
                    "to enable uniform treatment between specified "
                    "GOOGLE_APPLICATION_CREDENTIALS and use of Google's "
                    "Application Default Credentials."
                ),
                category=DeprecationWarning,
            )

        if self.using_application_default_credentials:
            logger.debug("Using application default credentials")
            creds = compute_engine.Credentials()

        elif os.path.exists(self.environment_variable_value):
            logger.debug("Using credentials from file %s", self.environment_variable_value)
            creds = self._get_credentials_from_file(self.environment_variable_value, as_dict=as_dict)

        else:
            warnings.warn(
                message=(
                    "Placing service account credentials JSON directly in an "
                    "environment variable will be deprecated soon. Set "
                    "GOOGLE_APPLICATION_CREDENTIALS to a file path, or leave "
                    "unset to use Application Default Credentials. "
                    "See https://medium.com/datamindedbe/application-default-credentials-477879e31cb5 "
                    "for a helpful overview of google credentials"
                ),
                category=DeprecationWarning,
            )

            logger.debug("Using credentials from environment string")
            try:
                creds = self._get_credentials_from_string(self.environment_variable_value, as_dict=as_dict)
            except JSONDecodeError as e:
                raise InvalidInputException(
                    f"The environment variable {self.environment_variable_name} has a value, but is neither a valid file path not valid json. Value is: {self.environment_variable_value}"
                ) from e

        return creds

    def _get_credentials_from_file(self, filename, as_dict=False):
        """Get the credentials from the JSON file whose path is specified in the environment variable's value.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials:
        """
        with open(filename, encoding="utf-8") as f:
            credentials = json.load(f)

        logger.debug("GCP credentials read from file.")

        if as_dict:
            return credentials

        return service_account.Credentials.from_service_account_info(credentials)

    def _get_credentials_from_string(self, value, as_dict=False):
        """Get the credentials directly from the JSON string specified in the environment variable's value.

        :param bool as_dict: if `True`, get the credentials as a dictionary
        :return dict|google.auth.service_account.Credentials:
        """
        credentials = json.loads(value)
        logger.debug("GCP credentials loaded from string.")

        if as_dict:
            return credentials

        return service_account.Credentials.from_service_account_info(credentials)
