import asyncio
from unittest.mock import patch
import socketio
from tests.base import BaseTestCase

from octue.resources.service import MockClient, Service


class TestService(BaseTestCase):
    def test_error_raised_when_client_cannot_connect(self):
        """ Test that a ConnectionError is raised if the service cannot connect to the given URI. """
        uri = "http://0.0.0.0:9999"
        with self.assertRaises(socketio.exceptions.ConnectionError) as error:
            Service(name="test_service", id=0, uri=uri)
            self.assertTrue(f"Failed to connect to server at {uri} using namespaces ['/octue']" in error)

    def test_ask(self):
        """ Test that a service can be asked a question. """
        with patch("socketio.Client", new=MockClient):
            service = Service(name="test_service", id=0, uri="http://0.0.0.0:8080")
            response = asyncio.run(service.ask({"what": "now"}))

        self.assertEqual(response, {"what": "now"})
