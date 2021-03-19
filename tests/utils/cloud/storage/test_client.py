import json
import tempfile
from unittest.mock import patch
import google.api_core.exceptions

from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestUploadFileToGoogleCloud(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.FILENAME = "my_file.txt"
        cls.storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    def test_upload_and_download_file(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded again."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            upload_local_path = f"{temporary_directory}/{self.FILENAME}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            self.storage_client.upload_file(
                local_path=upload_local_path,
                bucket_name=TEST_BUCKET_NAME,
                path_in_bucket=self.FILENAME,
            )

            download_local_path = f"{temporary_directory}/{self.FILENAME}-download"

            self.storage_client.download_to_file(
                bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME, local_path=download_local_path
            )

            with open(download_local_path) as f:
                self.assertTrue("This is a test upload." in f.read())

    def test_upload_file_fails_if_checksum_is_not_correct(self):
        """Test that uploading a file fails if its checksum isn't the correct."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            upload_local_path = f"{temporary_directory}/{self.FILENAME}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            with patch(
                "octue.utils.cloud.storage.client.GoogleCloudStorageClient._compute_crc32c_checksum",
                return_value="L3eGig==",
            ):

                with self.assertRaises(google.api_core.exceptions.BadRequest) as e:
                    self.storage_client.upload_file(
                        local_path=upload_local_path,
                        bucket_name=TEST_BUCKET_NAME,
                        path_in_bucket=self.FILENAME,
                    )

                self.assertTrue("doesn't match calculated CRC32C" in e.exception.message)

    def test_upload_from_string(self):
        """Test that a string can be uploaded to Google Cloud storage as a file and downloaded again."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            self.storage_client.upload_from_string(
                string=json.dumps({"height": 32}),
                bucket_name=TEST_BUCKET_NAME,
                path_in_bucket=self.FILENAME,
            )

            download_local_path = f"{temporary_directory}/{self.FILENAME}-download"

            self.storage_client.download_to_file(
                bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME, local_path=download_local_path
            )

            with open(download_local_path) as f:
                self.assertTrue('{"height": 32}' in f.read())

    def test_upload_from_string_fails_if_checksum_is_not_correct(self):
        """Test that uploading a string fails if its checksum isn't the correct."""
        with patch(
            "octue.utils.cloud.storage.client.GoogleCloudStorageClient._compute_crc32c_checksum",
            return_value="L3eGig==",
        ):
            with self.assertRaises(google.api_core.exceptions.BadRequest) as e:
                self.storage_client.upload_from_string(
                    string=json.dumps({"height": 15}),
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=self.FILENAME,
                )

            self.assertTrue("doesn't match calculated CRC32C" in e.exception.message)

    def test_download_as_string(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded as a string."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            upload_local_path = f"{temporary_directory}/{self.FILENAME}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            self.storage_client.upload_file(
                local_path=upload_local_path,
                bucket_name=TEST_BUCKET_NAME,
                path_in_bucket=self.FILENAME,
            )

        self.assertEqual(
            self.storage_client.download_as_string(bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME),
            "This is a test upload.",
        )

    def test_delete(self):
        """Test that a file can be deleted."""
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=self.FILENAME,
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME),
            ),
            {"height": 32},
        )

        self.storage_client.delete(bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME)

        with self.assertRaises(google.api_core.exceptions.NotFound):
            self.storage_client.download_as_string(bucket_name=TEST_BUCKET_NAME, path_in_bucket=self.FILENAME)

    def test_scandir(self):
        """Test that Google Cloud storage "directories"' contents can be listed."""
        directory_path = storage.path.join("my", "path")

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(directory_path, self.FILENAME),
        )

        contents = list(self.storage_client.scandir(bucket_name=TEST_BUCKET_NAME, directory_path=directory_path))
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0].name, storage.path.join(directory_path, self.FILENAME))

    def test_scandir_with_empty_directory(self):
        """Test that an empty directory shows as such."""
        directory_path = storage.path.join("another", "path")
        contents = list(self.storage_client.scandir(bucket_name=TEST_BUCKET_NAME, directory_path=directory_path))
        self.assertEqual(len(contents), 0)

    def test_get_metadata(self):
        """Test that file metadata can be retrieved."""
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=self.FILENAME,
        )

        metadata = self.storage_client.get_metadata(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=self.FILENAME,
        )

        self.assertTrue(len(metadata) > 0)
