import base64
import json
import logging
from crc32c import crc32
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
        blob = self._blob(bucket_name, path_in_bucket)

        with open(local_path) as f:
            blob.crc32c = self._compute_crc32c_checksum(f.read())

        blob.upload_from_filename(filename=local_path, timeout=timeout)
        self._update_metadata(blob, metadata)
        logger.info("Uploaded %r to Google Cloud at %r.", local_path, blob.public_url)

    def upload_from_string(self, serialised_data, bucket_name, path_in_bucket, metadata=None, timeout=_DEFAULT_TIMEOUT):
        """Upload serialised data in string form to a file in a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        blob = self._blob(bucket_name, path_in_bucket)
        blob.crc32c = self._compute_crc32c_checksum(serialised_data)

        blob.upload_from_string(data=serialised_data, timeout=timeout)
        self._update_metadata(blob, metadata)
        logger.info("Uploaded data to Google Cloud at %r.", blob.public_url)

    def download_to_file(self, bucket_name, path_in_bucket, local_path, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a file from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        blob = self._blob(bucket_name, path_in_bucket)
        blob.download_to_filename(local_path, timeout=timeout)
        logger.info("Downloaded %r from Google Cloud to %r.", blob.public_url, local_path)

    def download_as_string(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a string from a Google Cloud bucket at
        https://storage.cloud.google.com/<bucket_name>/<path_in_bucket>
        """
        blob = self._blob(bucket_name, path_in_bucket)
        data = blob.download_as_bytes(timeout=timeout)
        logger.info("Downloaded %r from Google Cloud to as string.", blob.public_url)
        return data.decode()

    def get_metadata(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Get the metadata of the given file in the given bucket."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        metadata = bucket.get_blob(blob_name=self._strip_leading_slash(path_in_bucket), timeout=timeout)._properties

        if metadata["metadata"] is not None:
            metadata["metadata"] = {key: json.loads(value) for key, value in metadata["metadata"].items()}

        return metadata

    def delete(self, bucket_name, path_in_bucket, timeout=_DEFAULT_TIMEOUT):
        """Delete the given file from the given bucket."""
        blob = self._blob(bucket_name, path_in_bucket)
        blob.delete(timeout=timeout)
        logger.info("Deleted %r from Google Cloud.", blob.public_url)

    def scandir(self, bucket_name, directory_path, filter=None, timeout=_DEFAULT_TIMEOUT):
        """Yield the blobs belonging to the given "directory" in the given bucket."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blobs = bucket.list_blobs(timeout=timeout)
        directory_path = self._strip_leading_slash(directory_path)

        if filter:
            return (blob for blob in blobs if blob.name.startswith(directory_path) and filter(blob))

        return (blob for blob in blobs if blob.name.startswith(directory_path))

    def _strip_leading_slash(self, path):
        """Strip the leading slash from a path."""
        return path.lstrip("/")

    def _blob(self, bucket_name, path_in_bucket):
        """Instantiate a blob for the given bucket at the given path. Note that this is not synced up with Google Cloud."""
        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        return bucket.blob(blob_name=self._strip_leading_slash(path_in_bucket))

    def _compute_crc32c_checksum(self, string):
        """Compute the CRC32 checksum of the string."""
        checksum = crc32(string.encode())
        return base64.b64encode(checksum.to_bytes(length=4, byteorder="big")).decode("utf-8")

    def _update_metadata(self, blob, metadata):
        """Update the metadata for the given blob. Note that this is synced up with Google Cloud."""
        blob.metadata = self._encode_metadata(metadata or {})
        blob.patch()

    def _encode_metadata(self, metadata):
        """Encode metadata as a dictionary of JSON strings."""
        if not isinstance(metadata, dict):
            raise TypeError(f"Metadata for Google Cloud storage should be a dictionary; received {metadata!r}")

        return {key: json.dumps(value) for key, value in metadata.items()}
