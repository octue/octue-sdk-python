from gcloud_storage_emulator.server import create_server
from google.cloud import storage

from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import upload_file_to_google_cloud
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

    def test_upload_file_to_bucket(self):
        """Test that a file can be uploaded to Google Cloud storage."""
        upload_url = upload_file_to_google_cloud(
            local_path="file_to_upload.txt",
            project_name=self.PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket="file_to_upload.txt",
        )

        self.assertEqual(upload_url, f"https://storage.cloud.google.com/{self.TEST_BUCKET_NAME}/file_to_upload.txt")
