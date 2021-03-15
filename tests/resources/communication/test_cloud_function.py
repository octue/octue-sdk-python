import base64
import json
import os
import unittest
import uuid
from datetime import datetime
from unittest import TestCase

from octue.resources.communication.google_cloud_function.main import run_analysis
from tests import TEST_PROJECT_NAME, TESTS_DIR


class TestRunAnalysis(TestCase):
    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = unittest.mock.MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.pubsub.topic.publish"

    def test_run_analysis(self):
        """Test that the cloud function run an analysis and publishes the results to Pub/Sub."""
        os.environ["GCP_PROJECT"] = TEST_PROJECT_NAME
        os.environ["SERVICE_ID"] = str(uuid.uuid4())

        deployment_configuration = {
            "app_dir": os.path.join(TESTS_DIR, "resources", "communication"),
            "twine": os.path.join(TESTS_DIR, "resources", "communication", "twine.json"),
        }

        event = {
            "data": base64.b64encode(
                json.dumps({"input_values": {"n_iterations": 3}, "input_manifest": None}).encode()
            ),
            "attributes": {"question_uuid": str(uuid.uuid4())},
            "messageId": "1",
            "publishTime": str(datetime.now()),
            "orderingKey": "",
        }

        run_analysis(event=event, context=self._make_mock_context(), deployment_configuration=deployment_configuration)
