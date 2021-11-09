import json
import os
import tempfile
from unittest.mock import patch
import google.oauth2.service_account

from octue.cloud.credentials import GCPCredentialsManager
from tests.base import BaseTestCase


class TestGCPCredentialsManager(BaseTestCase):
    def test_warning_issued_if_no_environment_variable(self):
        """Ensure a warning is issued if the given environment variable can't be found."""
        with self.assertWarns(Warning):
            GCPCredentialsManager(environment_variable_name="heeby_jeeby-11111-33103")

    def test_credentials_can_be_loaded_from_file(self):
        """Test that credentials can be loaded from the file at the path specified by the environment variable."""
        credentials = GCPCredentialsManager().get_credentials()
        self.assertEqual(type(credentials), google.oauth2.service_account.Credentials)

    def test_credentials_can_be_loaded_as_dictionary_from_string(self):
        """Test that credentials can be loaded as a dictionary from a string-valued environment variable."""
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": '{"blah": "nah"}'}):
            credentials = GCPCredentialsManager().get_credentials(as_dict=True)
            self.assertEqual(credentials, {"blah": "nah"})

    def test_credentials_can_be_loaded_as_dictionary_from_file(self):
        """Test that credentials can be loaded as a dictionary from a file."""
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:

            with open(temporary_file.name, "w") as f:
                json.dump({"blah": "nah"}, f)

            with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": temporary_file.name}):
                credentials = GCPCredentialsManager().get_credentials(as_dict=True)
                self.assertEqual(credentials, {"blah": "nah"})

    def test_credentials_are_none_when_environment_variable_name_is_none(self):
        """Test that credentials are `None` when the given environment variable name is `None`."""
        self.assertIsNone(GCPCredentialsManager(environment_variable_name=None).get_credentials())
