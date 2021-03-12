import os
import unittest

from octue.utils.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier


TESTS_DIR = os.path.dirname(__file__)


test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier()
setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)
