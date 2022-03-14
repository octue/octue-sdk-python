import json
import logging
import os
import tempfile
from unittest.mock import patch
from google.auth import compute_engine
import google.oauth2.service_account

from octue.exceptions import InvalidInputException
from octue.cloud.credentials import GCPCredentialsManager
from tests.base import BaseTestCase


class TestGCPCredentialsManager(BaseTestCase):
    def test_error_raised_if_environment_variable_set_to_none(self):
        """Ensure an error is raised in the case that someone explicitly overrides the default value to None"""
        with self.assertRaises(InvalidInputException):
            GCPCredentialsManager(environment_variable_name=None)

    def test_error_raised_if_environment_variable_is_unset(self):
        """Ensure an error is raised if the given environment variable can't be found."""
        with self.assertRaises(InvalidInputException):
            mgr = GCPCredentialsManager(environment_variable_name="AN_UNSET_ENVVAR")

    def test_error_raised_if_environment_variable_is_invalid(self):
        """Ensure an error is raised if the given environment variable can't be found."""
        with patch.dict(os.environ, {"A_LEGIT_ENVVAR_NAME": "with-a-value-neither-file-nor-json"}):
            with self.assertRaises(InvalidInputException):
                mgr = GCPCredentialsManager(environment_variable_name="A_LEGIT_ENVVAR_NAME")
                mgr.get_credentials()

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

    def test_compute_engine_credentials_are_used_when_environment_unset(self):
        """Test that application default credentials are accessed when the environment variable value is not set."""
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": ""}):
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            creds = GCPCredentialsManager().get_credentials()
            self.assertIsInstance(creds, compute_engine.credentials.Credentials)
