import base64
import json
import logging
from google.cloud import storage
from google.cloud.storage.constants import _DEFAULT_TIMEOUT
from google_crc32c import Checksum

from octue.cloud.credentials import GCPCredentialsManager
from octue.cloud.storage.path import split_bucket_name_from_gs_path
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)

OCTUE_MANAGED_CREDENTIALS = "octue-managed"


class GoogleCloudStorageClient:
    """A client for using Google Cloud Storage.

    :param str project_name:
    :param str|google.auth.credentials.Credentials credentials:
    :return None:
    """

    def __init__(self, project_name, credentials=OCTUE_MANAGED_CREDENTIALS):
        if credentials == OCTUE_MANAGED_CREDENTIALS:
            credentials = GCPCredentialsManager().get_credentials()
        else:
            credentials = credentials

        self.client = storage.Client(project=project_name, credentials=credentials)
        self.project_name = project_name

    def create_bucket(self, name, location=None, allow_existing=False, timeout=_DEFAULT_TIMEOUT):
        """Create a new bucket. If the bucket already exists, and `allow_existing` is `True`, do nothing; if it is
        `False`, raise an error.

        :param str name:
        :param str|None location: physical region of bucket; e.g. "europe-west6"; defaults to "US"
        :param bool allow_existing:
        :param float timeout:
        :raise google.cloud.exceptions.Conflict:
        :return None:
        """
        if allow_existing:
            if self.client.lookup_bucket(bucket_name=name, timeout=timeout) is not None:
                return

        self.client.create_bucket(bucket_or_name=name, location=location, timeout=timeout)

    def upload_file(
        self,
        local_path,
        cloud_path=None,
        bucket_name=None,
        path_in_bucket=None,
        metadata=None,
        timeout=_DEFAULT_TIMEOUT,
    ):
        """Upload a local file to a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>. Either (`bucket_name`
        and `path_in_bucket`) or `cloud_path` must be provided.

        :param str local_path: path to local file
        :param str|None cloud_path: full cloud path to upload file to (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket to store file in
        :param str|None path_in_bucket: path to upload file to (e.g. `path/to/file.csv`)
        :param dict metadata: key-value pairs to associate with the cloud file as metadata
        :param float timeout: time in seconds to allow for the upload to complete
        :return None:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)

        with open(local_path) as f:
            blob.crc32c = self._compute_crc32c_checksum(f.read())

        blob.upload_from_filename(filename=local_path, timeout=timeout)
        self._overwrite_blob_custom_metadata(blob, metadata)
        logger.debug("Uploaded %r to Google Cloud at %r.", local_path, blob.public_url)

    def upload_from_string(
        self, string, cloud_path=None, bucket_name=None, path_in_bucket=None, metadata=None, timeout=_DEFAULT_TIMEOUT
    ):
        """Upload serialised data in string form to a file in a Google Cloud bucket at
        gs://<bucket_name>/<path_in_bucket>. Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be provided.

        :param str string: string to upload as file
        :param str|None cloud_path: full cloud path to upload as file to (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket to store as file in
        :param str|None path_in_bucket: path to upload as file to (e.g. `path/to/file.csv`)
        :param dict metadata: key-value pairs to associate with the cloud file as metadata
        :param float timeout: time in seconds to allow for the upload to complete
        :return None:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)
        blob.crc32c = self._compute_crc32c_checksum(string)

        blob.upload_from_string(data=string, timeout=timeout)
        self._overwrite_blob_custom_metadata(blob, metadata)
        logger.debug("Uploaded data to Google Cloud at %r.", blob.public_url)

    def get_metadata(self, cloud_path=None, bucket_name=None, path_in_bucket=None, timeout=_DEFAULT_TIMEOUT):
        """Get the metadata of the given file in the given bucket. Either (`bucket_name` and `path_in_bucket`) or
        `cloud_path` must be provided.

        :param str|None cloud_path: full cloud path to file (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket where cloud file is located
        :param str|None path_in_bucket: path to cloud file (e.g. `path/to/file.csv`)
        :param float timeout: time in seconds to allow for the request to complete
        :return dict:
        """
        if cloud_path:
            bucket_name, path_in_bucket = split_bucket_name_from_gs_path(cloud_path)

        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blob = bucket.get_blob(blob_name=self._strip_leading_slash(path_in_bucket), timeout=timeout)

        if blob is None:
            return None

        custom_metadata = {}

        if blob.metadata:
            for key, value in blob.metadata.items():
                try:
                    custom_metadata[key] = json.loads(value, cls=OctueJSONDecoder)
                except json.decoder.JSONDecodeError:
                    custom_metadata[key] = value

        return {
            "custom_metadata": custom_metadata,
            "crc32c": blob.crc32c,
            "size": blob.size,
            "updated": blob.updated,
            "time_created": blob.time_created,
            "time_deleted": blob.time_deleted,
            "custom_time": blob.custom_time,
            "project_name": self.project_name,
            "bucket_name": bucket_name,
            "path_in_bucket": path_in_bucket,
        }

    def overwrite_custom_metadata(self, metadata, cloud_path=None, bucket_name=None, path_in_bucket=None):
        """Overwrite the custom metadata for the given cloud file. Either (`bucket_name` and `path_in_bucket`) or
        `cloud_path` must be provided.

        :param dict metadata: key-value pairs to set as the new custom metadata
        :param str|None cloud_path: full cloud path to file (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket where cloud file is located
        :param str|None path_in_bucket: path to cloud file (e.g. `path/to/file.csv`)
        :return None:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)
        self._overwrite_blob_custom_metadata(blob, metadata)

    def download_to_file(
        self, local_path, cloud_path=None, bucket_name=None, path_in_bucket=None, timeout=_DEFAULT_TIMEOUT
    ):
        """Download a file to a file from a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>. Either
        (`bucket_name` and `path_in_bucket`) or `cloud_path` must be provided.

        :param str local_path: path to download to
        :param str|None cloud_path: full cloud path to download from (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket cloud file is stored in
        :param str|None path_in_bucket: path to download from (e.g. `path/to/file.csv`)
        :param float timeout: time in seconds to allow for the download to complete
        :return None:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)
        blob.download_to_filename(local_path, timeout=timeout)
        logger.debug("Downloaded %r from Google Cloud to %r.", blob.public_url, local_path)

    def download_as_string(self, cloud_path=None, bucket_name=None, path_in_bucket=None, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a string from a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>. Either
        (`bucket_name` and `path_in_bucket`) or `cloud_path` must be provided.

        :param str|None cloud_path: full cloud path to download from (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket cloud file is stored in
        :param str|None path_in_bucket: path to download from (e.g. `path/to/file.csv`)
        :param float timeout: time in seconds to allow for the download to complete
        :return str:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)
        data = blob.download_as_bytes(timeout=timeout)
        logger.debug("Downloaded %r from Google Cloud to as string.", blob.public_url)
        return data.decode()

    def delete(self, cloud_path=None, bucket_name=None, path_in_bucket=None, timeout=_DEFAULT_TIMEOUT):
        """Delete the given file from the given bucket. Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must
        be provided.

        :param str|None cloud_path: full cloud path to file to delete (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket cloud file is stored in
        :param str|None path_in_bucket: path to file to delete (e.g. `path/to/file.csv`)
        :param float timeout: time in seconds to allow for the request to complete
        :return None:
        """
        blob = self._blob(cloud_path, bucket_name, path_in_bucket)
        blob.delete(timeout=timeout)
        logger.debug("Deleted %r from Google Cloud.", blob.public_url)

    def scandir(self, cloud_path=None, bucket_name=None, directory_path=None, filter=None, timeout=_DEFAULT_TIMEOUT):
        """Yield the blobs belonging to the given "directory" in the given bucket. Either (`bucket_name` and
        `path_in_bucket`) or `cloud_path` must be provided.

        :param str|None cloud_path: full cloud path of directory to scan (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket cloud directory is located in
        :param str|None directory_path: path of cloud directory to scan (e.g. `path/to/file.csv`)
        :param callable filter: blob filter to constrain the yielded results
        :param float timeout: time in seconds to allow for the request to complete
        :yield google.cloud.storage.blob.Blob:
        """
        if cloud_path:
            bucket_name, directory_path = split_bucket_name_from_gs_path(cloud_path)

        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        blobs = bucket.list_blobs(timeout=timeout)
        directory_path = self._strip_leading_slash(directory_path)

        if filter:
            return (blob for blob in blobs if blob.name.startswith(directory_path) and filter(blob))

        return (blob for blob in blobs if blob.name.startswith(directory_path))

    def _strip_leading_slash(self, path):
        """Strip the leading slash from a path.

        :param str path:
        :return str:
        """
        return path.lstrip("/")

    def _blob(self, cloud_path=None, bucket_name=None, path_in_bucket=None):
        """Instantiate a blob for the given bucket at the given path. Note that this is not synced up with Google Cloud.
        Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be provided.

        :param str|None cloud_path:
        :param str|None bucket_name:
        :param str|None path_in_bucket:
        :return google.cloud.storage.blob.Blob:
        """
        if cloud_path:
            bucket_name, path_in_bucket = split_bucket_name_from_gs_path(cloud_path)

        bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        return bucket.blob(blob_name=self._strip_leading_slash(path_in_bucket))

    def _compute_crc32c_checksum(self, string):
        """Compute the CRC32 checksum of the string.

        :param str string:
        :return str:
        """
        checksum = Checksum(string.encode())
        return base64.b64encode(checksum.digest()).decode("utf-8")

    def _overwrite_blob_custom_metadata(self, blob, metadata):
        """Overwrite the custom metadata for the given blob. Note that this is synced up with Google Cloud.

        :param google.cloud.storage.blob.Blob blob: Google Cloud Storage blob to update
        :param dict metadata: key-value pairs of metadata to overwrite the blob's metadata with
        :return None:
        """
        if not metadata:
            return None

        blob.metadata = self._encode_metadata(metadata)
        blob.patch()

    def _encode_metadata(self, metadata):
        """Encode metadata as a dictionary of JSON strings.

        :param dict metadata:
        :return dict:
        """
        if not isinstance(metadata, dict):
            raise TypeError(f"Metadata for Google Cloud storage should be a dictionary; received {metadata!r}")

        return {key: json.dumps(value, cls=OctueJSONEncoder) for key, value in metadata.items()}
