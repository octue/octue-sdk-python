import json
import os
import tempfile
from gcloud_storage_emulator.server import create_server
from google.cloud import storage

from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import OCTUE_MANAGED_CREDENTIALS, GoogleCloudStorageClient
from tests.base import BaseTestCase


class TestUploadFileToGoogleCloud(BaseTestCase):

    PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        cls.storage_emulator.start()
        storage.Client(project=cls.PROJECT_NAME, credentials=GCPCredentialsManager().get_credentials()).create_bucket(
            bucket_or_name=cls.TEST_BUCKET_NAME
        )

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()

    def test_upload_and_download_file(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded again."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME, credentials=OCTUE_MANAGED_CREDENTIALS)
        filename = "file_to_upload.txt"

        with tempfile.TemporaryDirectory() as temporary_directory:

            upload_local_path = f"{temporary_directory}/{filename}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            storage_client.upload_file(
                local_path=upload_local_path,
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=filename,
            )

            download_local_path = f"{temporary_directory}/{filename}-download"

            storage_client.download_to_file(
                bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename, local_path=download_local_path
            )

            with open(download_local_path) as f:
                self.assertTrue("This is a test upload." in f.read())

    def test_upload_and_download_from_string(self):
        """Test that a string can be uploaded to Google Cloud storage as a file and downloaded again."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME, credentials=OCTUE_MANAGED_CREDENTIALS)
        filename = "file_to_upload.txt"

        with tempfile.TemporaryDirectory() as temporary_directory:

            storage_client.upload_from_string(
                serialised_data=json.dumps({"height": 32}),
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=filename,
            )

            download_local_path = f"{temporary_directory}/{filename}-download"

            storage_client.download_to_file(
                bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename, local_path=download_local_path
            )

            with open(download_local_path) as f:
                self.assertTrue('{"height": 32}' in f.read())

    def test_upload_and_download_file_as_string(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded as a string."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME, credentials=OCTUE_MANAGED_CREDENTIALS)
        filename = "file_to_upload.txt"

        with tempfile.TemporaryDirectory() as temporary_directory:

            upload_local_path = f"{temporary_directory}/{filename}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            storage_client.upload_file(
                local_path=upload_local_path,
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=filename,
            )

        self.assertEqual(
            storage_client.download_as_string(bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename),
            "This is a test upload.",
        )
