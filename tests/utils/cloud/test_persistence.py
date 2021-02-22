import json
import os
import tempfile
import google.api_core.exceptions

from octue.utils.cloud.persistence import OCTUE_MANAGED_CREDENTIALS, GoogleCloudStorageClient
from tests.base import BaseTestCase


class TestUploadFileToGoogleCloud(BaseTestCase):
    PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]

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

    def test_delete(self):
        """Test that a file can be deleted."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME, credentials=OCTUE_MANAGED_CREDENTIALS)
        filename = "file_to_upload.txt"

        storage_client.upload_from_string(
            serialised_data=json.dumps({"height": 32}),
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket=filename,
        )

        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename
                ),
            ),
            {"height": 32},
        )

        storage_client.delete(bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename)

        with self.assertRaises(google.api_core.exceptions.NotFound):
            GoogleCloudStorageClient(project_name=self.PROJECT_NAME).download_as_string(
                bucket_name=self.TEST_BUCKET_NAME, path_in_bucket=filename
            )

    def test_get_metadata(self):
        """Test that file metadata can be retrieved."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME, credentials=OCTUE_MANAGED_CREDENTIALS)
        filename = "file_to_upload.txt"

        storage_client.upload_from_string(
            serialised_data=json.dumps({"height": 32}),
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket=filename,
        )

        metadata = storage_client.get_metadata(
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket=filename,
        )

        self.assertTrue(len(metadata) > 0)
