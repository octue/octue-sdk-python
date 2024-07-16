import json
import logging
from logging import makeLogRecord
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import MESSAGES, MockService
from octue.cloud.emulators.service import ServicePatcher
from octue.cloud.pub_sub.logging import GoogleCloudPubSubHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


class NonJSONSerialisable:
    def __repr__(self):
        return "NonJSONSerialisableInstance"


class TestGoogleCloudPubSubHandler(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        """Start the service patcher.

        :return None:
        """
        cls.service_patcher.start()

    @classmethod
    def tearDownClass(cls):
        """Stop the services patcher.

        :return None:
        """
        cls.service_patcher.stop()

    def test_emit(self):
        """Test the log message is published when `GoogleCloudPubSubHandler.emit` is called."""
        question_uuid = "96d69278-44ac-4631-aeea-c90fb08a1b2b"
        log_record = makeLogRecord({"msg": "Starting analysis."})
        service = MockService(backend=GCPPubSubBackend(project_name="blah"))

        GoogleCloudPubSubHandler(
            event_emitter=service._emit_event,
            question_uuid=question_uuid,
            parent_question_uuid=None,
            originator_question_uuid=question_uuid,
            parent="another/service:1.0.0",
            originator="another/service:1.0.0",
            recipient="another/service:1.0.0",
            retry_count=0,
        ).emit(log_record)

        self.assertEqual(
            json.loads(MESSAGES[question_uuid][0].data.decode())["log_record"]["msg"],
            "Starting analysis.",
        )

    def test_emit_with_non_json_serialisable_args(self):
        """Test that non-JSON-serialisable arguments to log messages are converted to their string representation
        before being serialised and published to the Pub/Sub topic.
        """
        question_uuid = "96d69278-44ac-4631-aeea-c90fb08a1b2b"
        non_json_serialisable_thing = NonJSONSerialisable()

        # Check that it can't be serialised to JSON.
        with self.assertRaises(TypeError):
            json.dumps(non_json_serialisable_thing)

        record = logging.makeLogRecord(
            {"msg": "%r is not JSON-serialisable but can go into a log message", "args": (non_json_serialisable_thing,)}
        )

        service = MockService(backend=GCPPubSubBackend(project_name="blah"))

        with patch("octue.cloud.emulators._pub_sub.MockPublisher.publish") as mock_publish:
            GoogleCloudPubSubHandler(
                event_emitter=service._emit_event,
                question_uuid=question_uuid,
                parent_question_uuid=None,
                originator_question_uuid=question_uuid,
                parent="another/service:1.0.0",
                originator="another/service:1.0.0",
                recipient="another/service:1.0.0",
                retry_count=0,
            ).emit(record)

        self.assertEqual(
            json.loads(mock_publish.call_args.kwargs["data"].decode())["log_record"]["msg"],
            "NonJSONSerialisableInstance is not JSON-serialisable but can go into a log message",
        )
