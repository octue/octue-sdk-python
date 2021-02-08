import google.oauth2.service_account

from octue.utils.cloud.credentials import GCPCredentialsManager
from tests.base import BaseTestCase


class TestGCPCredentialsManager(BaseTestCase):
    def test_error_is_raise_if_no_environment_variable(self):
        """ Ensure an error is raised if the given environment variable can't be found. """
        with self.assertRaises(EnvironmentError):
            GCPCredentialsManager(environment_variable_name="heeby_jeeby-11111-33103")

    def test_credentials_can_be_loaded_from_file(self):
        """ Test that credentials can be loaded from the file at the path specified by the environment variable. """
        credentials = GCPCredentialsManager().get_credentials()
        self.assertEqual(type(credentials), google.oauth2.service_account.Credentials)
