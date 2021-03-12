import os
import socket
from contextlib import closing
from gcp_storage_emulator.server import create_server


class GoogleCloudStorageEmulator:
    """A local emulator for Google Cloud Storage

    :param str host:
    :param int port:
    :param str default_bucket:
    :return None:
    """

    def __init__(self, host="localhost", port=9090, in_memory=True, default_bucket=os.environ["TEST_BUCKET_NAME"]):
        self._server = create_server(host, port, in_memory=in_memory, default_bucket=default_bucket)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        """Start the emulator. Do nothing if it's already started on the given host and port.

        :return None:
        """
        try:
            self._server.start()
        except RuntimeError:
            pass

    def stop(self):
        """Stop the emulator.

        :return None:
        """
        self._server.stop()


def get_free_tcp_port():
    """Get a free TCP port.

    :return int:
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        _, port = s.getsockname()
        return port


class TestResultModifier:
    """A class providing `startTestRun` and `endTestRun` methods for use by a `unittest.TestResult`. The methods run
    before and after all tests respectively.

    :return None:
    """

    def __init__(self):
        PORT = get_free_tcp_port()
        self.STORAGE_EMULATOR_HOST = f"http://localhost:{PORT}"
        self.storage_emulator = GoogleCloudStorageEmulator(port=PORT)

    def startTestRun(self):
        """Start the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        os.environ["STORAGE_EMULATOR_HOST"] = self.STORAGE_EMULATOR_HOST
        self.storage_emulator.start()

    def stopTestRun(self):
        """Stop the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        self.storage_emulator.stop()
        del os.environ["STORAGE_EMULATOR_HOST"]
