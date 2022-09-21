from octue.compatibility import is_compatible
from tests.base import BaseTestCase


class TestCompatibility(BaseTestCase):
    def test_compatible(self):
        """Test that compatibility is correctly determined as true for two known compatible versions."""
        self.assertTrue(is_compatible(parent_sdk_version="0.35.0", child_sdk_version="0.35.0"))

    def test_not_compatible(self):
        """Test that compatibility is correctly determined as false for two known incompatible versions."""
        self.assertFalse(is_compatible(parent_sdk_version="0.16.0", child_sdk_version="0.35.0"))

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
        """Test that two versions are compatible but a warning is raised if no data is available on the parent version."""
        with self.assertLogs() as logging_context:
            self.assertTrue(is_compatible(parent_sdk_version="0.26.0", child_sdk_version="1000000.0.0"))

        self.assertIn(
            "No data on compatibility of parent SDK version 0.26.0 and child SDK version 1000000.0.0.",
            logging_context.output[0],
        )
