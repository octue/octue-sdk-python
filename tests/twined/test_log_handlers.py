import logging
import os
from unittest.mock import patch

from octue import apply_log_handler
from octue.twined.log_handlers import AnalysisLogFormatterSwitcher
from tests.base import BaseTestCase
from tests.twined.test_app_modules.app_using_submodule.app import run as app_using_submodule


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
        self.assertEqual(logging_context.records[0].name, "tests.twined.test_app_modules.app_using_submodule.app")
        self.assertEqual(logging_context.records[0].message, "Log message from app.")

        self.assertIn("[hello-moto]", logging_context.output[1])
        self.assertEqual(logging_context.records[1].name, "tests.twined.test_app_modules.app_using_submodule.submodule")
        self.assertEqual(logging_context.records[1].message, "Log message from submodule.")
