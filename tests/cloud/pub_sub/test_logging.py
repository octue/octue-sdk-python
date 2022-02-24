import json
import logging
from logging import makeLogRecord
from unittest.mock import patch

from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import MESSAGES, MockService, MockTopic


class NonJSONSerialisable:
    def __repr__(self):
        return "NonJSONSerialisableInstance"


class TestGooglePubSubHandler(BaseTestCase):
    def test_emit(self):
        """Test the log message is published when `GooglePubSubHandler.emit` is called."""
        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)
        topic = MockTopic(name="world", namespace="hello", service=service)
        topic.create()

        log_record = makeLogRecord({"msg": "Starting analysis."})
        GooglePubSubHandler(service.publisher, topic, "analysis-id").emit(log_record)

        self.assertEqual(json.loads(MESSAGES[topic.name][0].data.decode())["log_record"]["msg"], "Starting analysis.")

    def test_emit_with_non_json_serialisable_args(self):
        """Test that non-JSON-serialisable arguments to log messages are converted to their string representation
        before being serialised and published to the Pub/Sub topic.
        """
        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)
        topic = MockTopic(name="world-1", namespace="hello", service=service)
        topic.create()

        non_json_serialisable_thing = NonJSONSerialisable()

        # Check that it can't be serialised to JSON.
        with self.assertRaises(TypeError):
            json.dumps(non_json_serialisable_thing)

        record = logging.makeLogRecord(
            {"msg": "%r is not JSON-serialisable but can go into a log message", "args": (non_json_serialisable_thing,)}
        )

        with patch("tests.cloud.pub_sub.mocks.MockPublisher.publish") as mock_publish:
            GooglePubSubHandler(service.publisher, topic, "analysis-id").emit(record)

        self.assertEqual(
            json.loads(mock_publish.call_args.kwargs["data"].decode())["log_record"]["msg"],
            "NonJSONSerialisableInstance is not JSON-serialisable but can go into a log message",
        )
