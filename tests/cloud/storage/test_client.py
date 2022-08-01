import datetime
import json
import os
import tempfile
from unittest.mock import patch

import google.api_core.exceptions
import google.cloud.exceptions
import requests

from octue.cloud import storage
from octue.cloud.emulators.cloud_storage import mock_generate_signed_url
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import CloudStorageBucketNotFound
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase


class TestGoogleCloudStorageClient(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the class by adding a cloud filename and a storage client to access the cloud storage emulator.

        :return None:
        """
        cls.FILENAME = "my_file.txt"
        cls.storage_client = GoogleCloudStorageClient()

    def test_create_bucket(self):
        """Test that a bucket can be created."""
        name = TEST_BUCKET_NAME + "-of-sand"
        self.assertIsNone(self.storage_client.client.lookup_bucket(name))
        self.storage_client.create_bucket(name)
        self.assertEqual(self.storage_client.client.lookup_bucket(name).name, name)

    def test_create_bucket_in_non_default_location(self):
        """Test that a bucket can be created in a non-default location."""
        name = TEST_BUCKET_NAME + "-of-chocolate"
        self.assertIsNone(self.storage_client.client.lookup_bucket(name))
        self.storage_client.create_bucket(name, location="EUROPE-WEST2")
        self.assertEqual(self.storage_client.client.lookup_bucket(name).name, name)

    def test_create_bucket_when_already_exists_and_existence_allowed(self):
        """Test that a bucket isn't re-created if it already exists and pre-existence is allowed."""
        name = TEST_BUCKET_NAME + "-of-grass"
        self.storage_client.create_bucket(name)
        self.assertIsNotNone(self.storage_client.client.lookup_bucket(name))
        self.storage_client.upload_from_string("hello", storage.path.generate_gs_path(name, "file.txt"))
        self.storage_client.create_bucket(name, allow_existing=True)
        self.assertEqual(
            self.storage_client.download_as_string(storage.path.generate_gs_path(name, "file.txt")), "hello"
        )

    def test_error_raised_when_creating_existing_bucket_and_existence_not_allowed(self):
        """Test that an error is raised when trying to create a bucket that already exists and pre-existence isn't
        allowed.
        """
        name = TEST_BUCKET_NAME + "-of-cucumbers"
        self.storage_client.create_bucket(name)
        self.assertIsNotNone(self.storage_client.client.lookup_bucket(name))

        with self.assertRaises(google.api_core.exceptions.Conflict):
            self.storage_client.create_bucket(name, allow_existing=False)

    def test_error_raised_if_bucket_not_found(self):
        """Test that an error is raised if a cloud storage bucket isn't found."""
        with self.assertRaises(CloudStorageBucketNotFound):
            self.storage_client.exists(cloud_path=storage.path.generate_gs_path("non-existing-bucket", "my_file.json"))

    def test_upload_and_download_file(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded again."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("This is a test upload.")

        self.storage_client.upload_file(
            local_path=temporary_file.name, cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        download_local_path = tempfile.NamedTemporaryFile().name

        self.storage_client.download_to_file(
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME), local_path=download_local_path
        )

        with open(download_local_path) as f:
            self.assertTrue("This is a test upload." in f.read())

    def test_upload_file_fails_if_checksum_is_not_correct(self):
        """Test that uploading a file fails if its checksum isn't the correct."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("This is a test upload.")

        with patch(
            "octue.cloud.storage.client.GoogleCloudStorageClient._compute_crc32c_checksum",
            return_value="L3eGig==",
        ):

            with self.assertRaises(google.api_core.exceptions.BadRequest) as e:
                self.storage_client.upload_file(
                    local_path=temporary_file.name,
                    cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME),
                )

            self.assertTrue("doesn't match calculated CRC32C" in e.exception.message)

    def test_upload_from_string(self):
        """Test that a string can be uploaded to Google Cloud storage as a file and downloaded again."""
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}), cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            self.storage_client.download_to_file(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME),
                local_path=temporary_file.name,
            )

        with open(temporary_file.name) as f:
            self.assertTrue('{"height": 32}' in f.read())

    def test_upload_from_string_fails_if_checksum_is_not_correct(self):
        """Test that uploading a string fails if its checksum isn't the correct."""
        with patch(
            "octue.cloud.storage.client.GoogleCloudStorageClient._compute_crc32c_checksum",
            return_value="L3eGig==",
        ):
            with self.assertRaises(google.api_core.exceptions.BadRequest) as e:
                self.storage_client.upload_from_string(
                    string=json.dumps({"height": 15}),
                    cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME),
                )

            self.assertTrue("doesn't match calculated CRC32C" in e.exception.message)

    def test_download_as_string(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded as a string."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("This is a test upload.")

        self.storage_client.upload_file(
            local_path=temporary_file.name, cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        self.assertEqual(
            self.storage_client.download_as_string(storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)),
            "This is a test upload.",
        )

    def test_download_all_files(self):
        """Test that all files from a directory can be downloaded together."""
        dataset_path = self.create_nested_cloud_dataset()

        with tempfile.TemporaryDirectory() as temporary_directory:
            GoogleCloudStorageClient().download_all_files(
                local_path=temporary_directory,
                cloud_path=dataset_path,
                recursive=True,
            )

            directory_contents = list(os.walk(temporary_directory))
            self.assertEqual(directory_contents[0][1], ["sub-directory"])
            self.assertEqual(set(directory_contents[0][2]), {"file_1.txt", "file_0.txt"})
            self.assertEqual(directory_contents[1][1], ["sub-sub-directory"])
            self.assertEqual(directory_contents[1][2], ["sub_file.txt"])
            self.assertEqual(directory_contents[2][2], ["sub_sub_file.txt"])

    def test_copy(self):
        """Test that a file can be copied from one cloud location to another."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("This is a test upload.")

        original_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        self.storage_client.upload_file(local_path=temporary_file.name, cloud_path=original_cloud_path)

        destination_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "file_in_new_location.txt")
        self.storage_client.copy(original_cloud_path, destination_cloud_path)

        self.assertEqual(self.storage_client.download_as_string(destination_cloud_path), "This is a test upload.")

    def test_delete(self):
        """Test that a file can be deleted."""
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}), cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)),
            ),
            {"height": 32},
        )

        self.storage_client.delete(storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME))

        with self.assertRaises(google.api_core.exceptions.NotFound):
            self.storage_client.download_as_string(storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME))

    def test_scandir(self):
        """Test that Google Cloud storage "directories"' contents can be listed."""
        directory_path = storage.path.join("my", "path")

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path, self.FILENAME),
        )

        contents = list(
            self.storage_client.scandir(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path))
        )
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0].name, storage.path.join(directory_path, self.FILENAME))

    def test_scandir_with_cloud_path(self):
        """Test that Google Cloud storage "directories"' contents can be listed when a cloud path is used."""
        cloud_directory_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "a", "path")

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}), cloud_path=storage.path.join(cloud_directory_path, self.FILENAME)
        )

        contents = list(self.storage_client.scandir(cloud_directory_path))
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0].name, "a/path/my_file.txt")

    def test_scandir_with_empty_directory(self):
        """Test that an empty directory shows as such."""
        directory_path = storage.path.join("another", "path")
        contents = list(
            self.storage_client.scandir(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path))
        )
        self.assertEqual(len(contents), 0)

    def test_scandir_with_directory_of_subdirectories_includes_subdirectories_by_default(self):
        """Test that subdirectories of the given directory are included by scandir by default."""
        directory_path = storage.path.join("the", "path")

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path, self.FILENAME),
        )

        # Add a file in a subdirectory.
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path, "sub_directory", "blah.txt"),
        )

        contents = list(
            self.storage_client.scandir(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path))
        )
        self.assertEqual(len(contents), 2)

        self.assertEqual(
            {blob.name for blob in contents},
            {
                storage.path.join(directory_path, self.FILENAME),
                storage.path.join(directory_path, "sub_directory", "blah.txt"),
            },
        )

    def test_scandir_with_directory_of_subdirectories_ignores_subdirectories_if_told_to(self):
        """Test that subdirectories of the given directory are ignored by scandir if told to."""
        directory_path = storage.path.join("my", "path")

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path, self.FILENAME),
        )

        # Add a file in a subdirectory.
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path, "sub_directory", "blah.txt"),
        )

        contents = list(
            self.storage_client.scandir(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, directory_path), recursive=False
            )
        )
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0].name, storage.path.join(directory_path, self.FILENAME))

    def test_scandir_on_bucket_root(self):
        """Test that scandir works if used on the root of a bucket."""
        bucket_name = TEST_BUCKET_NAME + "-for-scandir"
        self.storage_client.create_bucket(name=bucket_name)

        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(bucket_name, self.FILENAME),
        )

        # Add a file in a subdirectory.
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=storage.path.generate_gs_path(bucket_name, "sub_directory", "blah.txt"),
        )

        contents = list(self.storage_client.scandir(storage.path.generate_gs_path(bucket_name)))
        self.assertEqual(len(contents), 2)

        self.assertEqual(
            {blob.name for blob in contents},
            {self.FILENAME, storage.path.join("sub_directory", "blah.txt")},
        )

    def test_get_metadata(self):
        """Test that file metadata can be retrieved."""
        self.storage_client.upload_from_string(
            string=json.dumps({"height": 32}), cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        metadata = self.storage_client.get_metadata(
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        self.assertTrue(len(metadata) > 0)

    def test_get_metadata_with_gs_path(self):
        """Test that file metadata can be retrieved when a GS path is used."""
        gs_path = f"gs://{TEST_BUCKET_NAME}/{self.FILENAME}"
        self.storage_client.upload_from_string(string=json.dumps({"height": 32}), cloud_path=gs_path)

        metadata = self.storage_client.get_metadata(gs_path)
        self.assertTrue(len(metadata) > 0)

    def test_get_metadata_does_not_fail_on_non_json_encoded_metadata(self):
        """Test that non-JSON-encoded metadata does not cause getting of metadata to fail."""
        self.storage_client.upload_from_string(
            string="some stuff", cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        # Manually add metadata that isn't JSON encoded (the above method JSON-encodes any metadata given to it).
        bucket = self.storage_client.client.get_bucket(TEST_BUCKET_NAME)
        blob = bucket.get_blob(self.FILENAME)
        blob.metadata = {"this": "is-not-json-encoded"}
        blob.patch()

        metadata = self.storage_client.get_metadata(
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, self.FILENAME)
        )

        self.assertEqual(metadata["custom_metadata"], {"this": "is-not-json-encoded"})

    def test_exists(self):
        """Test that an existing cloud file shows as existing."""
        path_to_existing_file = storage.path.generate_gs_path(TEST_BUCKET_NAME, "blah")

        self.storage_client.upload_from_string(
            string="some stuff",
            cloud_path=path_to_existing_file,
        )

        self.assertTrue(self.storage_client.exists(cloud_path=path_to_existing_file))

    def test_not_exists(self):
        """Test that a non-existing cloud file shows as non-existing."""
        self.assertFalse(
            self.storage_client.exists(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "does", "not", "exist.json")
            )
        )

    def test_generate_signed_url(self):
        """Test that a signed URL to a cloud object can be generated and used to access the file."""
        cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "blah")
        self.storage_client.upload_from_string(string="some stuff", cloud_path=cloud_path, metadata={"my": "metadata"})

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", mock_generate_signed_url):
            url = self.storage_client.generate_signed_url(cloud_path, expiration=datetime.timedelta(seconds=1))

        # Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable isn't available.
        with patch.dict(os.environ, clear=True):

            # Test that the signed URL works.
            response = requests.get(url)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.text, "some stuff")

            # Check that the file's custom metadata is included in the headers. This assertion will be uncommented when
            # this issue https://github.com/oittaa/gcp-storage-emulator/issues/187 with the storage emulator is
            # resolved. See issue https://github.com/octue/octue-sdk-python/issues/489.

            # self.assertEqual(response.headers["x-goog-meta-my"], json.dumps("metadata"))
