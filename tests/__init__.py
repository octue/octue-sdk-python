import os
import unittest

from tests.emulators import GoogleCloudStorageEmulator


TESTS_DIR = os.path.dirname(__file__)
storage_emulator = GoogleCloudStorageEmulator()


def startTestRun(instance):
    """Start the test run, running any code in this function first.

    :param unittest.TestResult instance:
    :return None:
    """
    storage_emulator.start()


def stopTestRun(instance):
    """Finish the test run, running any code in this function first.

    :param unittest.TestResult instance:
    :return None:
    """
    storage_emulator.stop()


setattr(unittest.TestResult, "startTestRun", startTestRun)
setattr(unittest.TestResult, "stopTestRun", stopTestRun)
