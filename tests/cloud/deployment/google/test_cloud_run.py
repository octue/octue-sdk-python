import base64
import json
import os
import unittest
import uuid
from unittest import TestCase, mock

from octue.cloud.deployment.google import cloud_run
from octue.cloud.pub_sub.service import Service
from octue.exceptions import MissingServiceID
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME


cloud_run.app.testing = True


class TestCloudRun(TestCase):
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
                            "attributes": {"question_uuid": str(uuid.uuid4())},
                        },
                    },
                )

                self.assertEqual(response.status_code, 204)

    def test_error_is_raised_if_service_id_environment_variable_is_missing_or_empty(self):
        """Test that a MissingServiceID error is raised if the `SERVICE_ID` environment variable is missing or empty."""
        with mock.patch.dict(os.environ, clear=True):
            with self.assertRaises(MissingServiceID):
                cloud_run.answer_question(
                    project_name="a-project-name", data={}, question_uuid="8c859f87-b594-4297-883f-cd1c7718ef29"
                )

        with mock.patch.dict(os.environ, {"SERVICE_ID": ""}):
            with self.assertRaises(MissingServiceID):
                cloud_run.answer_question(
                    project_name="a-project-name", data={}, question_uuid="8c859f87-b594-4297-883f-cd1c7718ef29"
                )

    @unittest.skipUnless(
        condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
        reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
    )
    def test_cloud_run_deployment(self):
        """Test that the Google Cloud Run example deployment works, providing a service that can be asked questions and
        send responses.
        """
        service_to_ask = "octue.services.009ea106-dc37-4521-a8cc-3e0836064334"
        asker = Service(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        subscription, _ = asker.ask(service_id=service_to_ask, input_values={"n_iterations": 3})
        answer = asker.wait_for_answer(subscription)
        self.assertEqual(answer, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})
