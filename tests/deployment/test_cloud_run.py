import concurrent.futures
import os
import uuid
from unittest import TestCase, mock

from octue.deployment.google.cloud_run import run_analysis
from octue.resources.communication.google_pub_sub import Topic
from octue.resources.communication.google_pub_sub.service import OCTUE_NAMESPACE, Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME, TESTS_DIR


class TestRunAnalysis(TestCase):
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

        answering_service = Service(id=answering_service_id, backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Create answering service topic (this would have been created already in production).
        topic = Topic(name=answering_service_id, namespace=OCTUE_NAMESPACE, service=answering_service)
        topic.create()

        asking_service = Service(id=str(uuid.uuid4()), backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        with mock.patch("google.cloud.pubsub_v1.publisher.client.Client.publish"):
            subscription, question_uuid = asking_service.ask(
                service_id=answering_service_id, input_values=input_values, input_manifest=input_manifest
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            answer_future = executor.submit(asking_service.wait_for_answer, subscription)
            executor.submit(
                run_analysis,
                data={"input_values": input_values, "input_manifest": input_manifest},
                question_uuid=question_uuid,
                deployment_configuration=deployment_configuration,
            )

        answer = answer_future.result()
        self.assertEqual(answer, {"output_values": "It worked!", "output_manifest": None})
        topic.delete()
