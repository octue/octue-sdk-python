import os
import unittest
from unittest import TestCase

import twined.exceptions
from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import GCPPubSubBackend


class TestDeployment(TestCase):
    # This is the service ID of the example service deployed to Google Cloud Run.
    EXAMPLE_SERVICE_ID = "octue.services.afbf37e3-7650-4e79-bc8e-37c0c26eae13"

    @unittest.skipUnless(
        condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
        reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
    )
    def test_cloud_run_deployment_forwards_exceptions_to_asking_service(self):
        """Test that exceptions raised in the (remote) responding service are forwarded to and raised by the asker."""
        asker = Service(backend=GCPPubSubBackend(project_name=os.environ["TEST_PROJECT_NAME"]))
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
        asker = Service(backend=GCPPubSubBackend(project_name=os.environ["TEST_PROJECT_NAME"]))
        subscription, _ = asker.ask(service_id=self.EXAMPLE_SERVICE_ID, input_values={"n_iterations": 3})
        answer = asker.wait_for_answer(subscription)
        self.assertEqual(answer, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})
