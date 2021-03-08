import os
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
