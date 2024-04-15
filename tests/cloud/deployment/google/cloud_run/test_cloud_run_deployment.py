import os
import time
import unittest
import uuid
from unittest import TestCase

import twined.exceptions
from octue.cloud.pub_sub.bigquery import get_events
from octue.resources import Child, Manifest


EXAMPLE_SERVICE_SRUID = "octue/example-service-cloud-run:0.4.0"


@unittest.skipUnless(
    condition=os.getenv("RUN_CLOUD_RUN_DEPLOYMENT_TEST", "0").lower() == "1",
    reason="'RUN_CLOUD_RUN_DEPLOYMENT_TEST' environment variable is False or not present.",
)
class TestCloudRunDeployment(TestCase):
    # This is the service ID of the example service deployed to Google Cloud Run.
    child = Child(
        id=EXAMPLE_SERVICE_SRUID,
        backend={"name": "GCPPubSubBackend", "project_name": os.environ["TEST_PROJECT_NAME"]},
    )

    def test_forwards_exceptions_to_parent(self):
        """Test that exceptions raised in the (remote) responding service are forwarded to and raised by the asker."""
        with self.assertRaises(twined.exceptions.InvalidValuesContents):
            self.child.ask(input_values={"invalid_input_data": "hello"})

    def test_synchronous_question(self):
        """Test that the Google Cloud Run example deployment works, providing a service that can be asked questions and
        send responses.
        """
        answer = self.child.ask(input_values={"n_iterations": 3})

        # Check the output values.
        self.assertEqual(answer["output_values"], [1, 2, 3, 4, 5])

        # Check that the output dataset and its files can be accessed.
        with answer["output_manifest"].datasets["example_dataset"].files.one() as (datafile, f):
            self.assertEqual(f.read(), "This is some example service output.")

    def test_asynchronous_question(self):
        """Test asking an asynchronous question and retrieving the resulting events from the event store."""
        question_uuid = str(uuid.uuid4())
        answer = self.child.ask(input_values={"n_iterations": 3}, question_uuid=question_uuid, asynchronous=True)
        self.assertIsNone(answer)

        # Wait for question to complete.
        time.sleep(10)

        events = get_events(
            table_id="octue_sdk_python_test_dataset.service-events",
            sender=EXAMPLE_SERVICE_SRUID,
            question_uuid=question_uuid,
            kind="result",
        )

        # Check the output values.
        self.assertEqual(events[0]["event"]["output_values"], [1, 2, 3, 4, 5])

        # Check that the output dataset and its files can be accessed.
        output_manifest = Manifest.deserialise(events[0]["event"]["output_manifest"])

        with output_manifest.datasets["example_dataset"].files.one() as (datafile, f):
            self.assertEqual(f.read(), "This is some example service output.")
