import importlib
import logging
import os
import sys
from unittest import mock
from unittest.mock import patch

from octue.log_handlers import (
    LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP,
    LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP,
    AnalysisLogFormatterSwitcher,
    apply_log_handler,
    get_remote_handler,
)
from tests.base import BaseTestCase
from tests.test_app_modules.app_using_submodule.app import run as app_using_submodule


class TestLogging(BaseTestCase):
    def test_log_record_attributes_without_timestamp_used_if_compute_provider_is_google_cloud_run(self):
        """Test that the formatter without a timestamp is used for logging if the `COMPUTE_PROVIDER` environment
        variable is present and equal to "GOOGLE_CLOUD_RUN", and `USE_OCTUE_LOG_HANDLER` is equal to "1".
        """
        with mock.patch.dict(os.environ, USE_OCTUE_LOG_HANDLER="1", COMPUTE_PROVIDER="GOOGLE_CLOUD_RUN"):
            with mock.patch("octue.log_handlers.create_octue_formatter") as create_octue_formatter:
                importlib.reload(sys.modules["octue"])

        create_octue_formatter.assert_called_with(
            LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP,
            include_line_number=False,
            include_process_name=False,
            include_thread_name=False,
        )

    def test_log_record_attributes_with_timestamp_used_if_compute_provider_is_not_google_cloud_run(self):
        """Test that the formatter without a timestamp is used for logging if the `COMPUTE_PROVIDER` environment
        variable is present and not equal to "GOOGLE_CLOUD_RUN", and `USE_OCTUE_LOG_HANDLER` is equal to "1".
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


class TestAnalysisLogFormatterSwitcher(BaseTestCase):
    def test_octue_formatter_not_applied_to_existing_handler_if_use_octue_log_handler_environment_variable_is_0(self):
        """Test that the Octue formatter isn't applied to the logger's existing handler if the `USE_OCTUE_LOG_HANDLER`
        environment variable is set to "0".
        """
        with patch.dict(os.environ, {"USE_OCTUE_LOG_HANDLER": "0"}):
            root_logger = logging.getLogger()
            initial_formatter = root_logger.handlers[0].formatter

            analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
                analysis_id="hello-moto",
                logger=root_logger,
                analysis_log_level=logging.INFO,
            )

            with analysis_log_handler_switcher:
                analysis_formatter = root_logger.handlers[0].formatter
                self.assertIs(analysis_formatter, initial_formatter)
                self.assertNotIn("[hello-moto]", analysis_formatter._fmt)

            self.assertIs(root_logger.handlers[0].formatter, initial_formatter)

    def test_octue_formatter_applied_to_existing_handler(self):
        """Test that the Octue formatter is applied to an existing handler while in the context of the analysis log
        formatter switcher and that the logger has the same handler after exiting the context.
        """
        root_logger = logging.getLogger()
        initial_handler = root_logger.handlers[0]
        self.assertNotIn("[hello-moto]", initial_handler.formatter._fmt)

        analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
            analysis_id="hello-moto",
            logger=root_logger,
            analysis_log_level=logging.INFO,
        )

        with analysis_log_handler_switcher:
            self.assertIn("[hello-moto]", initial_handler.formatter._fmt)

        self.assertIs(root_logger.handlers[0], initial_handler)

    def test_extra_handlers_are_added_while_in_context_then_removed(self):
        """Test that extra handlers are added to the logger when in the context of the analysis log formatter switcher
        and are removed when the context is exited.
        """
        root_logger = logging.getLogger()
        initial_handler = root_logger.handlers[0]
        extra_handler = logging.Handler()

        analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
            analysis_id="hello-moto",
            logger=root_logger,
            analysis_log_level=logging.INFO,
            extra_log_handlers=[extra_handler],
        )

        with analysis_log_handler_switcher:
            self.assertIn(extra_handler, root_logger.handlers)

        self.assertNotIn(extra_handler, root_logger.handlers)
        self.assertIs(root_logger.handlers[0], initial_handler)

    def test_extra_handlers_have_initial_logger_formatter_if_use_octue_log_handler_environment_variable_is_0(self):
        """Test that extra handlers have the logger's initial handler's initial formatter if the `USE_OCTUE_LOG_HANDLER`
        environment variable is set to "0".
        """
        with patch.dict(os.environ, {"USE_OCTUE_LOG_HANDLER": "0"}):
            root_logger = logging.getLogger()
            initial_formatter = root_logger.handlers[0].formatter
            extra_handler = logging.Handler()

            analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
                analysis_id="hello-moto",
                logger=root_logger,
                analysis_log_level=logging.INFO,
                extra_log_handlers=[extra_handler],
            )

            with analysis_log_handler_switcher:
                for handler in root_logger.handlers:
                    self.assertIs(handler.formatter, initial_formatter)

            self.assertIs(root_logger.handlers[0].formatter, initial_formatter)

    def test_log_messages_handled_via_root_logger_are_capturable(self):
        """Test that log messages handled via the root logger are still capturable when in the context of the analysis
        log formatter switcher.
        """
        root_logger = logging.getLogger()
        apply_log_handler(logger=root_logger)

        analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
            analysis_id="hello-moto",
            logger=root_logger,
            analysis_log_level=logging.INFO,
        )

        with self.assertLogs() as logging_context:
            with analysis_log_handler_switcher:
                root_logger.info("Log message to be captured.")

        self.assertIn("[hello-moto]", logging_context.output[0])
        self.assertEqual(logging_context.records[0].message, "Log message to be captured.")

    def test_submodule_logs_are_handled_and_capturable(self):
        """Test that log messages from modules imported in the context of the analysis log formatter switcher are
        handled and capturable.
        """
        root_logger = logging.getLogger()
        apply_log_handler(logger=root_logger)

        analysis_log_handler_switcher = AnalysisLogFormatterSwitcher(
            analysis_id="hello-moto",
            logger=root_logger,
            analysis_log_level=logging.INFO,
        )

        with self.assertLogs(level=logging.INFO) as logging_context:
            with analysis_log_handler_switcher:
                app_using_submodule(None)

        self.assertIn("[hello-moto]", logging_context.output[0])
        self.assertEqual(logging_context.records[0].name, "tests.test_app_modules.app_using_submodule.app")
        self.assertEqual(logging_context.records[0].message, "Log message from app.")

        self.assertIn("[hello-moto]", logging_context.output[1])
        self.assertEqual(logging_context.records[1].name, "tests.test_app_modules.app_using_submodule.submodule")
        self.assertEqual(logging_context.records[1].message, "Log message from submodule.")
