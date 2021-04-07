import google.oauth2.service_account

from octue.cloud.credentials import GCPCredentialsManager
from tests.base import BaseTestCase


class TestGCPCredentialsManager(BaseTestCase):
    def test_warning_issued_if_no_environment_variable(self):
        """ Ensure a warning is issued if the given environment variable can't be found. """
        with self.assertWarns(Warning):
            GCPCredentialsManager(environment_variable_name="heeby_jeeby-11111-33103")

    def test_credentials_can_be_loaded_from_file(self):
        """ Test that credentials can be loaded from the file at the path specified by the environment variable. """
        credentials = GCPCredentialsManager().get_credentials()
        self.assertEqual(type(credentials), google.oauth2.service_account.Credentials)
