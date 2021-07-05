import importlib
import logging
import os
import sys
from unittest import mock

from octue.log_handlers import FORMATTER_WITHOUT_TIMESTAMP, get_remote_handler
from tests.base import BaseTestCase


class TestLogging(BaseTestCase):
    def test_compute_platform_environment_variable_triggers_correct_log_handler(self):
        """Test that the formatter without a timestamp is used for logging if the `COMPUTE_PROVIDER` environment
        variable is present and equal to "GOOGLE_CLOUD_RUN".
        """
        with mock.patch.dict(os.environ, COMPUTE_PROVIDER="GOOGLE_CLOUD_RUN"):
            with mock.patch("octue.log_handlers.apply_log_handler") as mock_apply_log_handler:
                importlib.reload(sys.modules["octue"])

        mock_apply_log_handler.assert_called_with(logger_name=None, formatter=FORMATTER_WITHOUT_TIMESTAMP)

    def test_octue_log_handler_not_used_if_use_octue_log_handler_environment_variable_not_present_or_0(self):
        """Test that the default octue log handler is not used if the `USE_OCTUE_LOG_HANDLER` environment variable is
        not present or is equal to "0".
        """
        with mock.patch.dict(os.environ, clear=True):
            with mock.patch("octue.log_handlers.apply_log_handler") as mock_apply_log_handler:
                importlib.reload(sys.modules["octue"])

        mock_apply_log_handler.assert_not_called()

        with mock.patch.dict(os.environ, USE_OCTUE_LOG_HANDLER="0"):
            with mock.patch("octue.log_handlers.apply_log_handler") as mock_apply_log_handler:
                importlib.reload(sys.modules["octue"])

        mock_apply_log_handler.assert_not_called()

    def test_octue_log_handler_used_if_use_octue_log_handler_environment_variable_is_1(self):
        """Test that the default octue log handler is used if the `USE_OCTUE_LOG_HANDLER` environment variable is
        present and equal to "1".
        """
        with mock.patch.dict(os.environ, USE_OCTUE_LOG_HANDLER="1"):
            with mock.patch("octue.log_handlers.apply_log_handler") as mock_apply_log_handler:
                importlib.reload(sys.modules["octue"])

        mock_apply_log_handler.assert_called_with(logger_name=None)


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
