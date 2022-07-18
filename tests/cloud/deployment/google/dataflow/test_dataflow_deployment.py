import os
import unittest

from octue.resources import Child
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
        child = Child(
            id="octue/example-service-dataflow",
            backend={"name": "GCPPubSubBackend", "project_name": os.environ["TEST_PROJECT_NAME"]},
        )

        answer = child.ask(input_values={"n_iterations": 3})
        self.assertEqual(answer, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})
