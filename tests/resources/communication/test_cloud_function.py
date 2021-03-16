import base64
import concurrent.futures
import json
import os
import uuid
from datetime import datetime
from unittest import TestCase, mock

from octue.resources.communication.google_cloud_function.main import run_analysis
from octue.resources.communication.google_pub_sub import Topic
from octue.resources.communication.google_pub_sub.service import OCTUE_NAMESPACE, Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME, TESTS_DIR


class TestRunAnalysis(TestCase):
    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = mock.MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.pubsub.topic.publish"

    def test_run_analysis(self):
        """Test that the cloud function runs an analysis and publishes the results to Pub/Sub."""
        os.environ["GCP_PROJECT"] = TEST_PROJECT_NAME

        answering_service_id = str(uuid.uuid4())
        os.environ["SERVICE_ID"] = answering_service_id

        deployment_configuration = {
            "app_dir": os.path.join(TESTS_DIR, "resources", "communication"),
            "twine": os.path.join(TESTS_DIR, "resources", "communication", "twine.json"),
        }

        input_values = {"n_iterations": 3}
        input_manifest = None

        answering_service = Service(id=answering_service_id, backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Create answering service topic (this would have been created already in production).
        topic = Topic(name=answering_service_id, namespace=OCTUE_NAMESPACE, service=answering_service)
        topic.create()

        asking_service = Service(id=str(uuid.uuid4()), backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        with mock.patch("google.cloud.pubsub_v1.publisher.client.Client.publish"):
            subscription, question_uuid = asking_service.ask(
                service_id=answering_service_id, input_values=input_values, input_manifest=input_manifest
            )

        # This is an example of a Google Pub/Sub event that the Cloud Function will receive when an asking service asks
        # a question.
        event = {
            "data": base64.b64encode(
                json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode()
            ),
            "attributes": {"question_uuid": question_uuid},
            "messageId": str(uuid.uuid4()),
            "publishTime": str(datetime.now()),
            "orderingKey": "",
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            answer_future = executor.submit(asking_service.wait_for_answer, subscription)
            executor.submit(
                run_analysis,
                event=event,
                context=self._make_mock_context(),
                deployment_configuration=deployment_configuration,
            )

        answer = answer_future.result()
        self.assertEqual(answer, {"output_values": "It worked!", "output_manifest": None})
        topic.delete()
