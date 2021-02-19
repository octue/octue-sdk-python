import logging
from google.cloud import storage
from google.cloud.storage.constants import _DEFAULT_TIMEOUT

from octue.utils.cloud.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)

OCTUE_MANAGED_CREDENTIALS = "octue-managed"
GOOGLE_CLOUD_STORAGE_URL = "https://storage.cloud.google.com"


class GoogleCloudStorageClient:
    def __init__(self, project_name, credentials=OCTUE_MANAGED_CREDENTIALS):
        if credentials == OCTUE_MANAGED_CREDENTIALS:
            credentials = GCPCredentialsManager().get_credentials()
        else:
            credentials = credentials

        self.client = storage.Client(project=project_name, credentials=credentials)

    def upload_file(self, local_path, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Upload a local file to a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        bucket.blob(blob_name=path_in_bucket).upload_from_filename(filename=local_path, timeout=timeout)
        upload_url = f"{GOOGLE_CLOUD_STORAGE_URL}/{bucket_name}/{path_in_bucket}"
        logger.info("Uploaded %r to Google Cloud at %r.", local_path, upload_url)
        return upload_url

    def upload_from_string(self, serialised_data, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Upload serialised data in string form to a file in a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        bucket.blob(blob_name=path_in_bucket).upload_from_string(data=serialised_data, timeout=timeout)
        upload_url = self._generate_resource_url(bucket_name, path_in_bucket)
        logger.info("Uploaded %r to Google Cloud at %r.", serialised_data, upload_url)
        return upload_url

    def download_to_file(self, bucket_name, path_in_bucket, local_path, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a file from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        bucket.blob(blob_name=path_in_bucket).download_to_filename(local_path, timeout=timeout)
        download_url = self._generate_resource_url(bucket_name, path_in_bucket)
        logger.info("Downloaded %r from Google Cloud to %r.", download_url, local_path)

    def download_as_string(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a string from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        data = bucket.blob(blob_name=path_in_bucket).download_as_string(timeout=timeout)
        download_url = self._generate_resource_url(bucket_name, path_in_bucket)
        logger.info("Downloaded %r from Google Cloud to as string.", download_url)
        return data.decode()

    def delete(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Delete the given file from the given bucket."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        bucket.blob(blob_name=path_in_bucket).delete(timeout=timeout)

    def _generate_resource_url(self, bucket_name, path_in_bucket):
        """Generate the URL for a resource in Google Cloud storage."""
        return f"{GOOGLE_CLOUD_STORAGE_URL}/{bucket_name}/{path_in_bucket}"
