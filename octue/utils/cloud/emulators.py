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

    STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME = "STORAGE_EMULATOR_HOST"

    def __init__(self):
        # if os.environ.get(self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME) is not None:
        #     self.storage_emulator_port_is_preset = True
        #     port = int(os.environ[self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME].split(":")[-1])
        # else:
        port = get_free_tcp_port()
        # self.storage_emulator_port_is_preset = False

        self.storage_emulator_host = f"http://localhost:{port}"
        self.storage_emulator = GoogleCloudStorageEmulator(port=port)

    def startTestRun(self):
        """Start the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        # if not self.storage_emulator_port_is_preset:
        os.environ[self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME] = self.storage_emulator_host
        self.storage_emulator.start()

    def stopTestRun(self):
        """Stop the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        self.storage_emulator.stop()
        # if not self.storage_emulator_port_is_preset:
        del os.environ[self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME]
