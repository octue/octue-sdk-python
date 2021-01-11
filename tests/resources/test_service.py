import asyncio
from unittest.mock import patch
from tests.base import BaseTestCase

from octue.resources.service import MockClient, Service


class TestService(BaseTestCase):
    def test_ask(self):
        """ Test that a service can be asked a question. """
        with patch("socketio.Client", new=MockClient):
            service = Service(name="test_service", id=0, uri="http://0.0.0.0:8080")
            response = asyncio.run(service.ask({"what": "now"}))

        self.assertEqual(response, {"what": "now"})
