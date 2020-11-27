import logging
from unittest import mock
from tests.base import BaseTestCase

from octue.logging_handlers import get_remote_logger_handler


class TestLoggingHandlers(BaseTestCase):
    def test_get_remote_logger_handler(self):
        """Assert that the remote logger handler parses URIs properly."""
        handler = get_remote_logger_handler(logger_uri="https://0.0.0.1:3000/log", log_level="DEBUG")
        assert handler.host == "0.0.0.1:3000"
        assert handler.url == "/log"

    def test_remote_logger_emits_messages(self):
        """Test that the remote logger handler emits messages."""
        logger = logging.getLogger("test-logger")
        logger.addHandler(get_remote_logger_handler(logger_uri="https://0.0.0.0:80/log", log_level="DEBUG"))

        with mock.patch("logging.handlers.HTTPHandler.emit") as mock_emit:
            logger.warning("Hello")
            mock_emit.assert_called()
            assert mock_emit.call_args.args[0].msg == "Hello"
