import json
from logging import makeLogRecord
from unittest.mock import patch

from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import MESSAGES, MockService, MockTopic


class TestGooglePubSubHandler(BaseTestCase):
    def test_emit_logs_exception_on_failure(self):
        """Test that the error handler is called when an exception is raised in `GooglePubSubHandler.emit`."""
        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)
        topic = MockTopic(name="world", namespace="hello", service=service)

        log_record = makeLogRecord({"msg": "Starting analysis."})

        with patch("octue.cloud.pub_sub.logging.GooglePubSubHandler.handleError") as mock_handle_error:
            GooglePubSubHandler(service.publisher, topic).emit(log_record)

        mock_handle_error.assert_called_with(log_record)

    def test_emit(self):
        """Test the log message is published when `GooglePubSubHandler.emit` is called."""
        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)
        topic = MockTopic(name="world", namespace="hello", service=service)
        topic.create()

        log_record = makeLogRecord({"msg": "Starting analysis."})
        GooglePubSubHandler(service.publisher, topic).emit(log_record)

        self.assertEqual(json.loads(MESSAGES[topic.name][0].data.decode())["log_record"]["msg"], "Starting analysis.")
