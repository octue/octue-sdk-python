import os
from gcloud_storage_emulator.server import create_server


class GoogleCloudStorageEmulator:
    def __init__(self):
        self._server = create_server("localhost", 9090, in_memory=True, default_bucket=os.environ["TEST_BUCKET_NAME"])

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        try:
            self._server.start()
        except RuntimeError:
            pass

    def stop(self):
        self._server.stop()
