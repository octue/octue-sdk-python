import base64
import json
import os
import tempfile
import unittest
import uuid
from unittest import TestCase, mock

import twined.exceptions
from octue.cloud.deployment.google import cloud_run
from octue.cloud.pub_sub.service import Service
from octue.exceptions import MissingServiceID
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.cloud.pub_sub.mocks import MockTopic


cloud_run.app.testing = True


class TestCloudRun(TestCase):
    # This is the service ID of the example service deployed to Google Cloud Run.
    EXAMPLE_SERVICE_ID = "octue.services.009ea106-dc37-4521-a8cc-3e0836064334"

    def test_post_to_index_with_no_payload_results_in_400_error(self):
        """Test that a 400 (bad request) error code is returned if no payload is sent to the Flask endpoint."""
        with cloud_run.app.test_client() as client:
            response = client.post("/")
            self.assertEqual(response.status_code, 400)

    def test_post_to_index_with_invalid_payload_results_in_400_error(self):
        """Test that a 400 (bad request) error code is returned if an invalid payload is sent to the Flask endpoint."""
        with cloud_run.app.test_client() as client:
            response = client.post("/", json={"some": "data"})
            self.assertEqual(response.status_code, 400)

            response = client.post("/", json={"message": "data"})
            self.assertEqual(response.status_code, 400)

    def test_post_to_index_with_valid_payload(self):
        """Test that the Flask endpoint returns a 204 (ok, no content) response to a valid payload."""
        with cloud_run.app.test_client() as client:
            with mock.patch("octue.cloud.deployment.google.cloud_run.answer_question"):

                response = client.post(
                    "/",
                    json={
                        "subscription": "projects/my-project/subscriptions/my-subscription",
                        "message": {
                            "data": base64.b64encode(
                                json.dumps({"input_values": [1, 2, 3], "input_manifest": None}).encode()
                            ).decode(),
                            "attributes": {"question_uuid": str(uuid.uuid4()), "forward_logs": "1"},
                        },
                    },
                )

                self.assertEqual(response.status_code, 204)

    def test_error_is_raised_if_service_id_environment_variable_is_missing_or_empty(self):
        """Test that a MissingServiceID error is raised if the `SERVICE_ID` environment variable is missing or empty."""
        with mock.patch.dict(os.environ, clear=True):
            with self.assertRaises(MissingServiceID):
                cloud_run.answer_question(
                    project_name="a-project-name",
                    question={"data": {}, "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"}},
                )

        with mock.patch.dict(os.environ, {"SERVICE_ID": ""}):
            with self.assertRaises(MissingServiceID):
                cloud_run.answer_question(
                    project_name="a-project-name",
                    question={"data": {}, "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"}},
                )

    def test_with_no_deployment_configuration_file(self):
        """Test that the Cloud Run `answer_question` function uses the default deployment values when a deployment
        configuration file is not provided.
        """
        with mock.patch.dict(os.environ, {"SERVICE_ID": self.EXAMPLE_SERVICE_ID}):
            with mock.patch("octue.cloud.deployment.google.cloud_run.Runner") as mock_runner:
                with mock.patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                    with mock.patch("octue.cloud.deployment.google.cloud_run.Service"):
                        cloud_run.answer_question(
                            project_name="a-project-name",
                            question={
                                "data": {},
                                "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                            },
                            credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
                        )

        mock_runner.assert_called_with(
            **{
                "app_src": ".",
                "twine": "twine.json",
                "configuration_values": None,
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "skip_checks": False,
                "project_name": "a-project-name",
            }
        )

    def test_with_deployment_configuration_file(self):
        """Test that the Cloud Run `answer_question` function uses the values in the deployment configuration file if it
        is provided.
        """
        with mock.patch.dict(
            os.environ,
            {
                "DEPLOYMENT_CONFIGURATION_PATH": tempfile.NamedTemporaryFile().name,
                "SERVICE_ID": self.EXAMPLE_SERVICE_ID,
            },
        ):
            with mock.patch(
                "octue.cloud.deployment.google.cloud_run._get_deployment_configuration",
                return_value={"app_dir": "/path/to/app_dir"},
            ):
                with mock.patch("octue.cloud.deployment.google.cloud_run.Runner") as mock_runner:
                    with mock.patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                        with mock.patch("octue.cloud.deployment.google.cloud_run.Service"):
                            cloud_run.answer_question(
                                project_name="a-project-name",
                                question={
                                    "data": {},
                                    "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                                },
                                credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
                            )

        mock_runner.assert_called_with(
            **{
                "app_src": "/path/to/app_dir",
                "twine": "twine.json",
                "configuration_values": None,
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "skip_checks": False,
                "project_name": "a-project-name",
            }
        )

    @unittest.skipUnless(
        condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
        reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
    )
    def test_cloud_run_deployment_forwards_exceptions_to_asking_service(self):
        """Test that exceptions raised in the (remote) responding service are forwarded to and raised by the asker."""
        asker = Service(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        subscription, _ = asker.ask(service_id=self.EXAMPLE_SERVICE_ID, input_values={"invalid_input_data": "hello"})

        with self.assertRaises(twined.exceptions.InvalidValuesContents):
            asker.wait_for_answer(subscription)

    @unittest.skipUnless(
        condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
        reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
    )
    def test_cloud_run_deployment(self):
        """Test that the Google Cloud Run example deployment works, providing a service that can be asked questions and
        send responses.
        """
        asker = Service(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        subscription, _ = asker.ask(service_id=self.EXAMPLE_SERVICE_ID, input_values={"n_iterations": 3})
        answer = asker.wait_for_answer(subscription)
        self.assertEqual(answer, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})
