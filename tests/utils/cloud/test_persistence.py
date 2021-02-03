import tempfile
from gcloud_storage_emulator.server import create_server
from google.cloud import storage

from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient
from tests.base import BaseTestCase


class TestUploadFileToGoogleCloud(BaseTestCase):

    PROJECT_NAME = "octue-amy"
    TEST_BUCKET_NAME = "octue-test-bucket"
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        cls.storage_emulator.start()
        client = storage.Client(project=cls.PROJECT_NAME, credentials=GCPCredentialsManager().get_credentials())
        client.create_bucket(bucket_or_name=cls.TEST_BUCKET_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()

    def test_upload_and_download_file(self):
        """Test that a file can be uploaded to Google Cloud storage and downloaded again."""
        storage_client = GoogleCloudStorageClient(project_name=self.PROJECT_NAME)
        filename = "file_to_upload.txt"

        with tempfile.TemporaryDirectory() as temporary_directory:

            upload_local_path = f"{temporary_directory}/{filename}"

            with open(upload_local_path, "w") as f:
                f.write("This is a test upload.")

            upload_url = storage_client.upload_file(
                local_path=upload_local_path,
                project_name=self.PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=filename,
            )

            self.assertEqual(upload_url, f"https://storage.cloud.google.com/{self.TEST_BUCKET_NAME}/{filename}")

            download_local_path = f"{temporary_directory}/{filename}-download"
            storage_client.download_file(self.TEST_BUCKET_NAME, path_in_bucket=filename, local_path=download_local_path)

            with open(download_local_path) as f:
                self.assertTrue("This is a test upload." in f.read())
