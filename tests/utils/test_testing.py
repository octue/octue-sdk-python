import os
from unittest import TestCase

from octue.utils.testing import load_test_fixture
from tests import TESTS_DIR


TEST_CRASH_DIAGNOSTICS_PATH = os.path.join(TESTS_DIR, "data", "test_crash_diagnostics")


class TestTesting(TestCase):
    def test_load_test_fixture_from_downloaded_crash_diagnostics(self):
        """Test that loading a test fixture from downloaded crash diagnostics works."""
        configuration_values, configuration_manifest, input_values, input_manifest = load_test_fixture(
            path=TEST_CRASH_DIAGNOSTICS_PATH,
            serialise=False,
        )

        self.assertEqual(configuration_values, {"getting": "ready"})
        self.assertIn("configuration_dataset", configuration_manifest.datasets)
        self.assertEqual(input_values, {"hello": "world"})
        self.assertIn("input_dataset", input_manifest.datasets)
