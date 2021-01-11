from unittest.mock import patch
from tests.base import BaseTestCase

from octue.resources.service import Service


class MockClient:
    def connect(self, uri, namespaces):
        pass

    def emit(self, event, data, callback, namespace):
        """ Return the data as it was provided. """
        return callback(data)


class TestService(BaseTestCase):
    def test_ask(self):
        """ Test that a service can be asked a question. """
        with patch("socketio.Client", new=MockClient):
            service = Service(name="test_service", id=0, uri="http://0.0.0.0:8080")
            response = service.ask({"what": "now"})

        self.assertEqual(response, {"what": "now"})
