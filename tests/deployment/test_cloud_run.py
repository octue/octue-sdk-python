import base64
import concurrent.futures
import json
import os
import uuid
from unittest import TestCase, mock

from octue.deployment.google import cloud_run
from octue.deployment.google.cloud_run import answer_question
from octue.resources.communication.google_pub_sub import Topic
from octue.resources.communication.google_pub_sub.service import OCTUE_NAMESPACE, Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME, TESTS_DIR


cloud_run.app.testing = True


class TestCloudRun(TestCase):
    def test_run_analysis(self):
        """Test that the cloud function runs an analysis and publishes the results to Pub/Sub."""
        os.environ["PROJECT_ID"] = TEST_PROJECT_NAME

        answering_service_id = str(uuid.uuid4())
        os.environ["SERVICE_ID"] = answering_service_id

        deployment_configuration = {
            "app_dir": os.path.join(TESTS_DIR, "deployment"),
            "twine": os.path.join(TESTS_DIR, "deployment", "twine.json"),
        }

        input_values = {"n_iterations": 3}
        input_manifest = None

        # Create answering service topic (this would have been created already in production).
        topic = Topic(
            name=answering_service_id,
            namespace=OCTUE_NAMESPACE,
            service=Service(id=answering_service_id, backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME)),
        )

        topic.create()

        asker = Service(id=str(uuid.uuid4()), backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        with mock.patch("google.cloud.pubsub_v1.publisher.client.Client.publish"):
            subscription, question_uuid = asker.ask(
                service_id=answering_service_id, input_values=input_values, input_manifest=input_manifest
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            answer_future = executor.submit(asker.wait_for_answer, subscription)
            executor.submit(
                answer_question,
                data={"input_values": input_values, "input_manifest": input_manifest},
                question_uuid=question_uuid,
                deployment_configuration=deployment_configuration,
            )

            answer = answer_future.result()

        self.assertEqual(answer, {"output_values": "It worked!", "output_manifest": None})
        topic.delete()

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
            with mock.patch("octue.deployment.google.cloud_run.run_analysis"):

                response = client.post(
                    "/",
                    json={
                        "message": {
                            "data": base64.b64encode(
                                json.dumps({"input_values": [1, 2, 3], "input_manifest": None}).encode()
                            ).decode(),
                            "attributes": {"question_uuid": str(uuid.uuid4())},
                        }
                    },
                )

                self.assertEqual(response.status_code, 204)
