import json
import logging
from logging import makeLogRecord
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import SUBSCRIPTIONS, MockService, MockSubscription, MockTopic
from octue.cloud.pub_sub.logging import GoogleCloudPubSubHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


class NonJSONSerialisable:
    def __repr__(self):
        return "NonJSONSerialisableInstance"


class TestGoogleCloudPubSubHandler(BaseTestCase):
    def test_emit(self):
        """Test the log message is published when `GoogleCloudPubSubHandler.emit` is called."""
        topic = MockTopic(name="octue.my-service.3-3-3", project_name="blah")
        topic.create()

        question_uuid = "96d69278-44ac-4631-aeea-c90fb08a1b2b"
        subscription = MockSubscription(name=f"octue.my-service.3-3-3.answers.{question_uuid}", topic=topic)
        subscription.create()

        log_record = makeLogRecord({"msg": "Starting analysis."})

        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)

        GoogleCloudPubSubHandler(
            message_sender=service._send_message,
            topic=topic,
            question_uuid=question_uuid,
            originator=service.id,
        ).emit(log_record)

        self.assertEqual(
            json.loads(SUBSCRIPTIONS[subscription.name][0].data.decode())["log_record"]["msg"],
            "Starting analysis.",
        )

    def test_emit_with_non_json_serialisable_args(self):
        """Test that non-JSON-serialisable arguments to log messages are converted to their string representation
        before being serialised and published to the Pub/Sub topic.
        """
        topic = MockTopic(name="octue.my-service.3-3-4", project_name="blah")
        topic.create()

        non_json_serialisable_thing = NonJSONSerialisable()

        # Check that it can't be serialised to JSON.
        with self.assertRaises(TypeError):
            json.dumps(non_json_serialisable_thing)

        record = logging.makeLogRecord(
            {"msg": "%r is not JSON-serialisable but can go into a log message", "args": (non_json_serialisable_thing,)}
        )

        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)

        with patch("octue.cloud.emulators._pub_sub.MockPublisher.publish") as mock_publish:
            GoogleCloudPubSubHandler(
                message_sender=service._send_message,
                topic=topic,
                question_uuid="question-uuid",
                originator=service.id,
            ).emit(record)

        self.assertEqual(
            json.loads(mock_publish.call_args.kwargs["data"].decode())["log_record"]["msg"],
            "NonJSONSerialisableInstance is not JSON-serialisable but can go into a log message",
        )
