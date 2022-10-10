import os
import unittest

from octue.resources import Child
from tests.base import BaseTestCase


@unittest.skipUnless(
    condition=os.getenv("RUN_DATAFLOW_DEPLOYMENT_TEST", "").lower() == "2",
    reason="'RUN_DATAFLOW_DEPLOYMENT_TEST' environment variable is False or not present.",
)
class TestDataflowDeployment(BaseTestCase):
    def test_sending_question_to_dataflow_streaming_job_child(self):
        """Test that a question can be sent to an existing Google Dataflow streaming job child and an answer received
        in response.
        """
        child = Child(
            id="octue/example-service-dataflow:latest",
            backend={"name": "GCPPubSubBackend", "project_name": os.environ["TEST_PROJECT_NAME"]},
        )

        answer = child.ask(input_values={"n_iterations": 3}, timeout=60)

        # Check the output values.
        self.assertEqual(answer["output_values"], [1, 2, 3, 4, 5])

        # Check that the output dataset and its files can be accessed.
        with answer["output_manifest"].datasets["example_dataset"].files.one() as (datafile, f):
            self.assertEqual(f.read(), "This is some example service output.")
