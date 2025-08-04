import importlib
import logging
import os
import sys
from unittest import mock

from octue.log_handlers import (
    LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP,
    LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP,
    get_remote_handler,
)
from tests.base import BaseTestCase


class TestLogging(BaseTestCase):
    def test_log_record_attributes_without_timestamp_used_if_compute_provider_is_google_cloud_function(self):
        """Test that the formatter without a timestamp is used for logging if the `COMPUTE_PROVIDER` environment
        variable is present and equal to "GOOGLE_CLOUD_FUNCTION", and `USE_OCTUE_LOG_HANDLER` is equal to "1".
        """
        with mock.patch.dict(os.environ, USE_OCTUE_LOG_HANDLER="1", COMPUTE_PROVIDER="GOOGLE_CLOUD_FUNCTION"):
            with mock.patch("octue.log_handlers.create_octue_formatter") as create_octue_formatter:
                importlib.reload(sys.modules["octue"])

        create_octue_formatter.assert_called_with(
            LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP,
            include_line_number=False,
            include_process_name=False,
            include_thread_name=False,
        )

    def test_log_record_attributes_with_timestamp_used_if_compute_provider_is_not_google_cloud_function(self):
        """Test that the formatter without a timestamp is used for logging if the `COMPUTE_PROVIDER` environment
        variable is present and not equal to "GOOGLE_CLOUD_FUNCTION", and `USE_OCTUE_LOG_HANDLER` is equal to "1".
        """
        with mock.patch.dict(os.environ, USE_OCTUE_LOG_HANDLER="1", COMPUTE_PROVIDER="BLAH"):
            with mock.patch("octue.log_handlers.create_octue_formatter") as create_octue_formatter:
                importlib.reload(sys.modules["octue"])

        create_octue_formatter.assert_called_with(
            LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP,
            include_line_number=False,
            include_process_name=False,
            include_thread_name=False,
        )

    def test_octue_log_handler_not_used_if_use_octue_log_handler_environment_variable_is_0(self):
        """Test that the default octue log handler is not used if the `USE_OCTUE_LOG_HANDLER` environment variable is
        not present or is equal to "0".
        """
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

        mock_apply_log_handler.assert_called()

    def test_extra_log_record_attributes_are_included_if_relevant_environment_variables_provided(self):
        """Test that the expected extra log record attributes are included in the log context if the relevant
        environment variables are provided.
        """
        with mock.patch.dict(
            os.environ,
            USE_OCTUE_LOG_HANDLER="1",
            INCLUDE_LINE_NUMBER_IN_LOGS="1",
            INCLUDE_PROCESS_NAME_IN_LOGS="1",
            INCLUDE_THREAD_NAME_IN_LOGS="1",
        ):
            with mock.patch("octue.log_handlers.apply_log_handler") as mock_apply_log_handler:
                importlib.reload(sys.modules["octue"])

                mock_apply_log_handler.assert_called_with(
                    logger_name=None,
                    include_line_number=True,
                    include_process_name=True,
                    include_thread_name=True,
                )


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
