import importlib
import logging
import sys
from unittest.mock import patch

from octue.compatibility import is_compatible, warn_if_incompatible
from tests.base import BaseTestCase


class TestIsCompatible(BaseTestCase):
    def test_compatible(self):
        """Test that compatibility is correctly determined as true for two known compatible versions."""
        self.assertTrue(is_compatible(parent_sdk_version="0.35.0", child_sdk_version="0.35.0"))

    def test_not_compatible(self):
        """Test that compatibility is correctly determined as false for two known incompatible versions."""
        self.assertFalse(is_compatible(parent_sdk_version="0.50.0", child_sdk_version="0.51.0"))

    def test_warning_issued_if_no_data_on_parent_version(self):
        """Test that two versions are marked as compatible but a warning is raised if no data is available on the parent
        version.
        """
        with self.assertLogs() as logging_context:
            self.assertTrue(is_compatible(parent_sdk_version="1000000.0.0", child_sdk_version="0.26.0"))

        self.assertIn(
            "No data on compatibility of parent SDK version 1000000.0.0 and child SDK version 0.26.0.",
            logging_context.output[0],
        )

    def test_warning_issued_if_no_data_on_child_version(self):
        """Test that two versions are compatible but a warning is raised if no data is available on the child version."""
        with self.assertLogs() as logging_context:
            self.assertTrue(is_compatible(parent_sdk_version="0.26.0", child_sdk_version="1000000.0.0"))

        self.assertIn(
            "No data on compatibility of parent SDK version 0.26.0 and child SDK version 1000000.0.0.",
            logging_context.output[0],
        )

    def test_warning_issued_if_version_compatibility_data_file_not_found(self):
        """Test that a warning is issued if the version compatibility data file can't be found at import time and that
        a further "no data" warning is raised when checking compatibility of two versions.
        """
        try:
            with self.assertLogs(level=logging.WARNING) as logging_context:
                with patch("builtins.open", side_effect=FileNotFoundError()):
                    importlib.reload(sys.modules["octue.compatibility"])

                self.assertIn("Version compatibility data could not be loaded.", logging_context.output[0])
                self.assertTrue(is_compatible("0.39.0", "0.39.0"))

                self.assertIn(
                    "No data on compatibility of parent SDK version 0.39.0 and child SDK version 0.39.0.",
                    logging_context.output[1],
                )

        # Ensure version compatibility data is available again.
        finally:
            importlib.reload(sys.modules["octue.compatibility"])


class TestWarnIfIncompatible(BaseTestCase):
    def test_warn_if_incompatible_with_missing_child_version_information(self):
        """Test that a warning is raised when calling `warn_if_incompatible` with missing child version information."""
        with self.assertLogs(level=logging.WARNING) as logging_context:
            warn_if_incompatible(parent_sdk_version="0.16.0", child_sdk_version=None)

        self.assertIn(
            "The child couldn't be checked for compatibility with this service because its Octue SDK version wasn't "
            "provided. Please update it to the latest Octue SDK version.",
            logging_context.output[0],
        )

    def test_warn_if_incompatible_with_missing_parent_version_information(self):
        """Test that a warning is raised when calling `warn_if_incompatible` with missing parent version information."""
        with self.assertLogs(level=logging.WARNING) as logging_context:
            warn_if_incompatible(parent_sdk_version=None, child_sdk_version="0.16.0")

        self.assertIn(
            "The parent couldn't be checked for compatibility with this service because its Octue SDK version wasn't "
            "provided. Please update it to the latest Octue SDK version.",
            logging_context.output[0],
        )

    def test_warn_if_incompatible_with_incompatible_versions(self):
        """Test that a warning is raised if incompatible versions are detected."""
        with self.assertLogs(level=logging.WARNING) as logging_context:
            warn_if_incompatible(parent_sdk_version="0.50.0", child_sdk_version="0.51.0")

        self.assertIn(
            "The parent's Octue SDK version 0.16.0 is incompatible with the child's version 0.35.0. Please update "
            "either or both to the latest version.",
            logging_context.output[0],
        )

    def test_warn_if_incompatible_with_compatible_versions(self):
        """Test that no warning is raised if compatible versions are detected."""
        no_warnings = False

        try:
            with self.assertLogs(level=logging.WARNING):
                warn_if_incompatible(parent_sdk_version="0.35.0", child_sdk_version="0.35.0")
        except AssertionError:
            no_warnings = True

        self.assertTrue(no_warnings)
