import os
import unittest
from unittest.mock import patch

from octue.cloud.deployment.google.dataflow.pipeline import dispatch_batch_job
from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestPipeline(BaseTestCase):
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

    def test_sending_question_to_dataflow_batch_job_child(self):
        project_name = os.environ["TEST_PROJECT_NAME"]

        with patch.dict(os.environ, clear=True):
            dispatch_batch_job(
                data={"input_values": {"n_iterations": 3}, "input_manifest": None},
                forward_logs=True,
                project_name=project_name,
                region="europe-west2",
                image_uri="eu.gcr.io/octue-amy/example-service-cloud-run:latest",
            )
