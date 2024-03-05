import os
import unittest
from unittest import TestCase

import twined.exceptions
from octue.resources import Child


@unittest.skipUnless(
    condition=os.getenv("RUN_CLOUD_RUN_DEPLOYMENT_TEST", "0").lower() == "1",
    reason="'RUN_CLOUD_RUN_DEPLOYMENT_TEST' environment variable is False or not present.",
)
class TestCloudRunDeployment(TestCase):
    # This is the service ID of the example service deployed to Google Cloud Run.
    child = Child(
        id="octue/example-service-cloud-run:0.3.2",
        backend={"name": "GCPPubSubBackend", "project_name": os.environ["TEST_PROJECT_NAME"]},
    )

    def test_cloud_run_deployment_forwards_exceptions_to_asking_service(self):
        """Test that exceptions raised in the (remote) responding service are forwarded to and raised by the asker."""
        with self.assertRaises(twined.exceptions.InvalidValuesContents):
            self.child.ask(input_values={"invalid_input_data": "hello"})

    def test_cloud_run_deployment(self):
        """Test that the Google Cloud Run example deployment works, providing a service that can be asked questions and
        send responses.
        """
        answer = self.child.ask(input_values={"n_iterations": 3})

        # Check the output values.
        self.assertEqual(answer["output_values"], [1, 2, 3, 4, 5])

        # Check that the output dataset and its files can be accessed.
        with answer["output_manifest"].datasets["example_dataset"].files.one() as (datafile, f):
            self.assertEqual(f.read(), "This is some example service output.")

    def test_cloud_run_deployment_asynchronously(self):
        """Test asking an asynchronous (BigQuery) question."""
        answer = self.child.ask(
            input_values={"n_iterations": 3},
            bigquery_table_id="octue-sdk-python.octue_sdk_python_test_dataset.question-events",
        )

        self.assertIsNone(answer)
