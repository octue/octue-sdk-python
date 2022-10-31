import os
from unittest import TestCase

from octue.utils.testing import load_test_fixture
from tests import TESTS_DIR


TEST_CRASH_DIAGNOSTICS_PATH = os.path.join(TESTS_DIR, "data", "crash_diagnostics")


class TestTesting(TestCase):
    def test_load_test_fixture_from_downloaded_crash_diagnostics(self):
        """Test that loading a test fixture from downloaded crash diagnostics works."""
        configuration_values, configuration_manifest, input_values, input_manifest, child_emulators = load_test_fixture(
            path=TEST_CRASH_DIAGNOSTICS_PATH
        )

        self.assertEqual(configuration_values, {"getting": "ready"})
        self.assertEqual(input_values, {"hello": "world"})

        self.assertEqual(len(child_emulators), 1)
        self.assertEqual(child_emulators[0].id, "octue/my-child:latest")
        self.assertEqual(
            child_emulators[0].messages[2:],
            [
                {"type": "monitor_message", "data": '{"sample": "data"}'},
                {"type": "result", "output_values": [1, 2, 3, 4, 5], "output_manifest": None},
            ],
        )
