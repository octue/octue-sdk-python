from tests.base import BaseTestCase

from octue.logging_handlers import get_remote_logger_handler


class TestLoggingHandlers(BaseTestCase):
    def test_get_remote_logger_handler(self):
        """Assert that the remote logger handler parses URIs properly."""
        handler = get_remote_logger_handler(logger_uri="https://0.0.0.1:3000/log", log_level="DEBUG")
        assert handler.host == "0.0.0.1:3000"
        assert handler.url == "/log"
