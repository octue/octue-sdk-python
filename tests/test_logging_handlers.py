import logging
from unittest import mock

from octue.log_handlers import get_remote_handler
from tests.base import BaseTestCase


class TestGetRemoteHandler(BaseTestCase):
    def test_get_remote_handler_parses_ws_properly(self):
        """Assert that the remote log handler parses URIs properly."""
        handler = get_remote_handler(logger_uri="ws://0.0.0.1:3000")
        assert handler.host == "0.0.0.1"
        assert handler.port == 3000

    def test_wss_is_supported(self):
        """Test that HTTPS is supported by the remote log handler."""
        handler = get_remote_handler(logger_uri="wss://0.0.0.1:3000/log")
        assert handler.host == "0.0.0.1"
        assert handler.port == 3000

    def test_non_ws_or_wss_protocol_raises_error(self):
        """Ensure an error is raised if a protocol other than HTTP or HTTPS is used for the logger URI."""
        with self.assertRaises(ValueError):
            get_remote_handler(logger_uri="https://0.0.0.1:3000/log")

    def test_remote_logger_emits_messages(self):
        """Test that the remote log handler emits messages."""
        logger = logging.getLogger("test-logger")
        logger.addHandler(get_remote_handler(logger_uri="wss://0.0.0.0:80"))
        logger.setLevel("DEBUG")

        with mock.patch("logging.handlers.SocketHandler.emit") as mock_emit:
            logger.debug("Hello")
            mock_emit.assert_called()
