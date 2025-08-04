import json
import unittest
from unittest.mock import patch

import requests

import octue.exceptions
from octue.twined.cloud.registry import get_default_sruid, raise_if_revision_not_registered


class TestGetDefaultSRUID(unittest.TestCase):
    SERVICE_REGISTRIES = [{"name": "Octue Registry", "endpoint": "https://blah.com/services"}]

    @classmethod
    def setUpClass(cls):
        cls.id_token_patch = patch("octue.twined.cloud.registry._get_google_cloud_id_token", return_value="some-token")
        cls.id_token_patch.start()

    @classmethod
    def tearDownClass(cls):
        cls.id_token_patch.stop()

    def test_error_raised_if_request_fails(self):
        """Test that an error is raised if the request to the service registry fails."""
        mock_response = requests.Response()
        mock_response.status_code = 403

        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(requests.HTTPError):
                get_default_sruid(
                    namespace="my-org",
                    name="my-service",
                    service_registries=self.SERVICE_REGISTRIES,
                )

    def test_error_raised_if_revision_not_found(self):
        """Test that an error is raised if no revision is found for the service in the given registries."""
        mock_response = requests.Response()
        mock_response.status_code = 404

        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(octue.exceptions.ServiceNotFound):
                get_default_sruid(
                    namespace="my-org",
                    name="my-service",
                    service_registries=self.SERVICE_REGISTRIES,
                )

    def test_get_latest_sruid(self):
        """Test that the latest SRUID for a service can be found."""
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps({"revision_tag": "1.3.9"}).encode()

        with patch("requests.get", return_value=mock_response):
            latest_sruid = get_default_sruid(
                namespace="my-org",
                name="my-service",
                service_registries=self.SERVICE_REGISTRIES,
            )

        self.assertEqual(latest_sruid, "my-org/my-service:1.3.9")

    def test_get_latest_sruid_when_not_in_first_registry(self):
        """Test that the latest SRUID for a service can be found when the service isn't in the first registry."""
        mock_failure_response = requests.Response()
        mock_failure_response.status_code = 404

        mock_success_response = requests.Response()
        mock_success_response.status_code = 200
        mock_success_response._content = json.dumps({"revision_tag": "1.3.9"}).encode()

        with patch("requests.get", side_effect=[mock_failure_response, mock_success_response]):
            latest_sruid = get_default_sruid(
                namespace="my-org",
                name="my-service",
                service_registries=self.SERVICE_REGISTRIES
                + [{"name": "Another Registry", "endpoint": "cats.com/services"}],
            )

        self.assertEqual(latest_sruid, "my-org/my-service:1.3.9")


class TestRaiseIfRevisionNotRegistered(unittest.TestCase):
    SERVICE_REGISTRIES = [{"name": "Octue Registry", "endpoint": "https://blah.com/services"}]

    @classmethod
    def setUpClass(cls):
        cls.id_token_patch = patch("octue.twined.cloud.registry._get_google_cloud_id_token", return_value="some-token")
        cls.id_token_patch.start()

    @classmethod
    def tearDownClass(cls):
        cls.id_token_patch.stop()

    def test_error_raised_if_request_fails(self):
        """Test that an error is raised if the request to the service registry fails."""
        mock_response = requests.Response()
        mock_response.status_code = 403

        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(requests.HTTPError):
                raise_if_revision_not_registered(
                    sruid="my-org/my-service:1.0.0",
                    service_registries=self.SERVICE_REGISTRIES,
                )

    def test_error_raised_if_revision_not_found(self):
        """Test that an error is raised if no revision is found for the service in the given registries."""
        mock_response = requests.Response()
        mock_response.status_code = 404

        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(octue.exceptions.ServiceNotFound):
                raise_if_revision_not_registered(
                    sruid="my-org/my-service:1.0.0",
                    service_registries=self.SERVICE_REGISTRIES,
                )

    def test_no_error_raised_if_service_revision_registered(self):
        """Test that no error is raised if a revision is found for the service in the given registries."""
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps({"revision_tag": "1.0.0"}).encode()

        with patch("requests.get", return_value=mock_response):
            raise_if_revision_not_registered(
                sruid="my-org/my-service:1.0.0",
                service_registries=self.SERVICE_REGISTRIES,
            )
