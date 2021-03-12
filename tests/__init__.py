import os
import socket
import unittest
from contextlib import closing

from tests.emulators import GoogleCloudStorageEmulator


TESTS_DIR = os.path.dirname(__file__)


def get_free_tcp_port():
    """Get a free TCP port.

    :return int:
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        _, port = s.getsockname()
        return port


def startTestRun(instance):
    """Start the test run, running any code in this function first.

    :param unittest.TestResult instance:
    :return None:
    """
    os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST
    storage_emulator.start()


def stopTestRun(instance):
    """Finish the test run, running any code in this function first.

    :param unittest.TestResult instance:
    :return None:
    """
    storage_emulator.stop()
    del os.environ["STORAGE_EMULATOR_HOST"]


PORT = get_free_tcp_port()
STORAGE_EMULATOR_HOST = f"http://localhost:{PORT}"
storage_emulator = GoogleCloudStorageEmulator(port=PORT)

setattr(unittest.TestResult, "startTestRun", startTestRun)
setattr(unittest.TestResult, "stopTestRun", stopTestRun)
