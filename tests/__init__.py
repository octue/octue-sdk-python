import os
import unittest

from octue.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier


TESTS_DIR = os.path.dirname(__file__)
TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
TEST_BUCKET_NAME = "octue-test-bucket"

test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)
