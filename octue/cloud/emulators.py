import datetime
import logging
import os
import socket
from contextlib import closing

from gcp_storage_emulator.server import create_server
from google.cloud.storage.blob import _API_ACCESS_ENDPOINT


# Silence the GCP storage emulator logger below `ERROR` level messages.
emulator_logger = logging.getLogger("gcp_storage_emulator.server")
emulator_logger.setLevel(logging.ERROR)


class GoogleCloudStorageEmulator:
    """A local emulator for Google Cloud Storage

    :param str host:
    :param int port:
    :param str default_bucket:
    :return None:
    """

    def __init__(self, host="localhost", port=9090, in_memory=True, default_bucket=None):
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


class GoogleCloudStorageEmulatorTestResultModifier:
    """A class providing `startTestRun` and `endTestRun` methods for use by a `unittest.TestResult` that start up a
    Google Cloud Storage emulator on a free port before the tests run and stop it after they've all run.

    :param str host:
    :param bool in_memory:
    :param str default_bucket_name:
    :return None:
    """

    STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME = "STORAGE_EMULATOR_HOST"

    def __init__(self, host="localhost", in_memory=True, default_bucket_name=None):
        port = get_free_tcp_port()
        self.storage_emulator_host = f"http://{host}:{port}"

        self.storage_emulator = GoogleCloudStorageEmulator(
            host=host, port=port, in_memory=in_memory, default_bucket=default_bucket_name
        )

    def startTestRun(self):
        """Start the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        os.environ[self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME] = self.storage_emulator_host
        self.storage_emulator.start()

    def stopTestRun(self):
        """Stop the Google Cloud Storage emulator before starting the test run.

        :param unittest.TestResult test_result:
        :return None:
        """
        self.storage_emulator.stop()
        del os.environ[self.STORAGE_EMULATOR_HOST_ENVIRONMENT_VARIABLE_NAME]


def mock_generate_signed_url(blob, expiration=datetime.timedelta(days=7), **kwargs):
    """Mock generating a signed URL for a Google Cloud Storage blob. Signed URLs can't currently be generated when using
    workload identity federation, which we use for our CI tests.

    :param google.cloud.storage.blob.Blob blob:
    :param datetime.datetime|datetime.timedelta expiration:
    :return str:
    """
    mock_signed_query_parameter = (
        f"?Expires={round((datetime.datetime.now() + expiration).timestamp())}&GoogleAccessId=my-service-account%40my-p"
        f"roject.iam.gserviceaccount.com&Signature=mock-signature"
    )

    base_url = "/".join((kwargs.get("api_access_endpoint", _API_ACCESS_ENDPOINT), blob.bucket.name, blob.name))
    return base_url + mock_signed_query_parameter
