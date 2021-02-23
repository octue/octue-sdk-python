import os
import unittest

from octue.utils.cloud.emulators import GoogleCloudStorageEmulator


TESTS_DIR = os.path.dirname(__file__)
storage_emulator = GoogleCloudStorageEmulator()


def startTestRun(instance):
    storage_emulator.start()


def stopTestRun(instance):
    storage_emulator.stop()


setattr(unittest.TestResult, "startTestRun", startTestRun)
setattr(unittest.TestResult, "stopTestRun", stopTestRun)
