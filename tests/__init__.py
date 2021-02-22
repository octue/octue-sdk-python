import os
import unittest

from tests.emulators import GoogleCloudStorageEmulator


TESTS_DIR = os.path.dirname(__file__)
storage_emulator = GoogleCloudStorageEmulator()


def startTestRun(self):
    storage_emulator.start()


setattr(unittest.TestResult, "startTestRun", startTestRun)


def stopTestRun(self):
    storage_emulator.stop()


setattr(unittest.TestResult, "stopTestRun", stopTestRun)
