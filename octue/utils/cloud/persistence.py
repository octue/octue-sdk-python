import json
import logging
from google.cloud import storage
from google.cloud.storage.constants import _DEFAULT_TIMEOUT

from octue.utils.cloud.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)

OCTUE_MANAGED_CREDENTIALS = "octue-managed"


class GoogleCloudStorageClient:
    def __init__(self, project_name, credentials=OCTUE_MANAGED_CREDENTIALS):
        if credentials == OCTUE_MANAGED_CREDENTIALS:
            credentials = GCPCredentialsManager().get_credentials()
        else:
            credentials = credentials

        self.client = storage.Client(project=project_name, credentials=credentials)

    def upload_file(self, local_path, bucket_name, path_in_bucket, metadata=None, timeout=_DEFAULT_TIMEOUT):
        """Upload a local file to a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.blob(blob_name=path_in_bucket)
        blob.upload_from_filename(filename=local_path, timeout=timeout)
        blob.metadata = self._encode_metadata(metadata)
        blob.patch()
        logger.info("Uploaded %r to Google Cloud at %r.", local_path, blob.public_url)
        return blob.public_url

    def upload_from_string(self, serialised_data, bucket_name, path_in_bucket, metadata=None, timeout=_DEFAULT_TIMEOUT):
        """Upload serialised data in string form to a file in a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.blob(blob_name=path_in_bucket)
        blob.upload_from_string(data=serialised_data, timeout=timeout)
        blob.metadata = self._encode_metadata(metadata)
        blob.patch()
        logger.info("Uploaded data to Google Cloud at %r.", blob.public_url)
        return blob.public_url

    def download_to_file(self, bucket_name, path_in_bucket, local_path, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a file from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.blob(blob_name=path_in_bucket)
        blob.download_to_filename(local_path, timeout=timeout)
        logger.info("Downloaded %r from Google Cloud to %r.", blob.public_url, local_path)

    def download_as_string(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a string from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.blob(blob_name=path_in_bucket)
        data = blob.download_as_string(timeout=timeout)
        logger.info("Downloaded %r from Google Cloud to as string.", blob.public_url)
        return data.decode()

    def get_metadata(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Get the metadata of the given file in the given bucket."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        metadata = bucket.get_blob(blob_name=path_in_bucket, timeout=timeout)._properties

        if metadata["metadata"] is not None:
            metadata["metadata"] = {key: json.loads(value) for key, value in metadata["metadata"].items()}

        return metadata

    def delete(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Delete the given file from the given bucket."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.blob(blob_name=path_in_bucket)
        blob.delete(timeout=timeout)
        logger.info("Deleted %r from Google Cloud.", blob.public_url)

    def _encode_metadata(self, metadata):
        """Encode metadata as a dictionary of JSON strings."""
        if not isinstance(metadata, dict):
            raise TypeError(f"Metadata for Google Cloud storage should be a dictionary; received {metadata!r}")

        return {key: json.dumps(value) for key, value in metadata.items()}
