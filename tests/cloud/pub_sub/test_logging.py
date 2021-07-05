import json
from logging import makeLogRecord

from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import MESSAGES, MockService, MockTopic


class TestGooglePubSubHandler(BaseTestCase):
    def test_emit(self):
        """Test the log message is published when `GooglePubSubHandler.emit` is called."""
        backend = GCPPubSubBackend(project_name="blah")
        service = MockService(backend=backend)
        topic = MockTopic(name="world", namespace="hello", service=service)
        topic.create()

        log_record = makeLogRecord({"msg": "Starting analysis."})
        GooglePubSubHandler(service.publisher, topic).emit(log_record)

        self.assertEqual(json.loads(MESSAGES[topic.name][0].data.decode())["log_record"]["msg"], "Starting analysis.")
