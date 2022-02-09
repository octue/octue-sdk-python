import os
import unittest

from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "1",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestDataflowDeployment(BaseTestCase):
    def test_sending_question_to_dataflow_streaming_job_child(self):
        """Test that a question can be sent to an existing Google Dataflow streaming job child and an answer received
        in response.
        """
        parent = Service(backend=GCPPubSubBackend(project_name=os.environ["TEST_PROJECT_NAME"]))

        subscription, _ = parent.ask(
            service_id="octue.services.1df81225-7e87-4b1c-9413-cdc375a127a7",
            input_values={"n_iterations": 3},
        )

        answer = parent.wait_for_answer(subscription, timeout=3600)
        self.assertEqual(answer, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})
