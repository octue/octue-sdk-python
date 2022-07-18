import base64
import datetime
import json
import logging
import os
import warnings

import google.api_core.exceptions
import google.auth.exceptions
from google import auth
from google.auth import compute_engine
from google.auth.transport import requests as google_requests
from google.cloud import storage
from google.cloud.storage.constants import _DEFAULT_TIMEOUT
from google_crc32c import Checksum

from octue.cloud.storage.path import split_bucket_name_from_cloud_path
from octue.exceptions import CloudStorageBucketNotFound
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)


OCTUE_MANAGED_CREDENTIALS = "octue-managed"


class GoogleCloudStorageClient:
    """A client for using Google Cloud Storage.

    :param str|google.auth.credentials.Credentials|None credentials:
    :return None:
    """

    def __init__(self, credentials=OCTUE_MANAGED_CREDENTIALS):
        warnings.simplefilter("ignore", category=ResourceWarning)

        if credentials == OCTUE_MANAGED_CREDENTIALS:
            self.credentials, self.project_name = auth.default()
        else:
            self.credentials = credentials

        self.client = storage.Client(project=self.project_name, credentials=self.credentials)

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

    def exists(self, cloud_path):
        """Check if a file exists at the given path.

        :param str cloud_path: full cloud path to the file (e.g. `gs://bucket_name/path/to/file.csv`)
        :return bool: `True` if the file exists
        """
        blob = self._blob(cloud_path=cloud_path)
        return blob.exists()

    def upload_file(self, local_path, cloud_path, metadata=None, timeout=_DEFAULT_TIMEOUT):
        """Upload a local file to a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>.

        :param str local_path: path to local file
        :param str cloud_path: full cloud path to upload file to (e.g. `gs://bucket_name/path/to/file.csv`)
        :param dict|None metadata: key-value pairs to associate with the cloud file as metadata
        :param float timeout: time in seconds to allow for the upload to complete
        :return None:
        """
        blob = self._blob(cloud_path)

        with open(local_path, "rb") as f:
            blob.crc32c = self._compute_crc32c_checksum(f.read())

        if metadata:
            blob.metadata = self._encode_metadata(metadata)

        blob.upload_from_filename(filename=local_path, timeout=timeout)
        logger.debug("Uploaded %r to Google Cloud at %r.", local_path, blob.public_url)

    def upload_from_string(self, string, cloud_path, metadata=None, timeout=_DEFAULT_TIMEOUT):
        """Upload serialised data in string form to a file in a Google Cloud bucket at
        gs://<bucket_name>/<path_in_bucket>.

        :param str string: string to upload as file
        :param str cloud_path: full cloud path to upload as file to (e.g. `gs://bucket_name/path/to/file.csv`)
        :param dict|None metadata: key-value pairs to associate with the cloud file as metadata
        :param float timeout: time in seconds to allow for the upload to complete
        :return None:
        """
        blob = self._blob(cloud_path)
        blob.crc32c = self._compute_crc32c_checksum(string)

        if metadata:
            blob.metadata = self._encode_metadata(metadata)

        blob.upload_from_string(data=string, timeout=timeout)
        logger.debug("Uploaded data to Google Cloud at %r.", blob.public_url)

    def get_metadata(self, cloud_path, timeout=_DEFAULT_TIMEOUT):
        """Get the metadata of the given file in the given bucket.

        :param str cloud_path: full cloud path to file (e.g. `gs://bucket_name/path/to/file.csv`)
        :param float timeout: time in seconds to allow for the request to complete
        :return dict|None: `None` if the bucket or file don't exist
        """
        try:
            bucket, path_in_bucket = self._get_bucket_and_path_in_bucket(cloud_path)
        except CloudStorageBucketNotFound:
            return None

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
        }

    def overwrite_custom_metadata(self, cloud_path, metadata=None):
        """Overwrite the custom metadata for the given cloud file. If no metadata is given, the custom metadata is
        erased.

        :param str cloud_path: full cloud path to file (e.g. `gs://bucket_name/path/to/file.csv`)
        :param dict|None metadata: key-value pairs to set as the new custom metadata
        :return None:
        """
        blob = self._blob(cloud_path)
        blob.metadata = self._encode_metadata(metadata or {})
        blob.patch()

    def download_to_file(self, local_path, cloud_path, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a file from a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>.

        :param str local_path: path to download to
        :param str cloud_path: full cloud path to download from (e.g. `gs://bucket_name/path/to/file.csv`)
        :param float timeout: time in seconds to allow for the download to complete
        :return None:
        """
        blob = self._blob(cloud_path)

        os.makedirs(os.path.abspath(os.path.dirname(local_path)), exist_ok=True)
        blob.download_to_filename(local_path, timeout=timeout)
        logger.debug("Downloaded %r from Google Cloud to %r.", blob.public_url, local_path)

    def download_as_string(self, cloud_path, timeout=_DEFAULT_TIMEOUT):
        """Download a file to a string from a Google Cloud bucket at gs://<bucket_name>/<path_in_bucket>.

        :param str cloud_path: full cloud path to download from (e.g. `gs://bucket_name/path/to/file.csv`)
        :param float timeout: time in seconds to allow for the download to complete
        :return str:
        """
        blob = self._blob(cloud_path)
        data = blob.download_as_bytes(timeout=timeout)
        logger.debug("Downloaded %r from Google Cloud to as string.", blob.public_url)
        return data.decode()

    def delete(self, cloud_path, timeout=_DEFAULT_TIMEOUT):
        """Delete the given file from the given bucket.

        :param str cloud_path: full cloud path to file to delete (e.g. `gs://bucket_name/path/to/file.csv`)
        :param float timeout: time in seconds to allow for the request to complete
        :return None:
        """
        blob = self._blob(cloud_path)
        blob.delete(timeout=timeout)
        logger.debug("Deleted %r from Google Cloud.", blob.public_url)

    def scandir(
        self,
        cloud_path,
        filter=None,
        recursive=True,
        show_directories_as_blobs=False,
        timeout=_DEFAULT_TIMEOUT,
    ):
        """Yield the blobs belonging to the given "directory" in the given bucket.

        :param str cloud_path: full cloud path of directory to scan (e.g. `gs://bucket_name/path/to/file.csv`)
        :param callable filter: blob filter to constrain the yielded results
        :param bool recursive: if True, include all files in the tree below the given cloud directory
        :param bool show_directories_as_blobs: if False, do not show directories as blobs (this doesn't affect inclusion of their contained files if `recursive` is True)
        :param float timeout: time in seconds to allow for the request to complete
        :yield google.cloud.storage.blob.Blob:
        """
        if filter is None:
            filter = lambda blob: True

        bucket, directory_path = self._get_bucket_and_path_in_bucket(cloud_path)

        if not directory_path.endswith("/"):
            directory_path += "/"

        if recursive:
            blobs = bucket.list_blobs(prefix=directory_path, timeout=timeout)
        else:
            blobs = bucket.list_blobs(prefix=directory_path, delimiter="/", timeout=timeout)

        for blob in blobs:
            if show_directories_as_blobs:
                if filter(blob):
                    yield blob

            else:
                # Ensure the blob is a file (not a directory blob).
                if filter(blob) and not blob.name.endswith("/"):
                    yield blob

    def generate_signed_url(self, cloud_path, expiration=datetime.timedelta(days=7)):
        """Generate a signed URL for accessing the object at the given cloud path that expires after the given
        expiration date or period.

        :param str cloud_path: the path to the object to generate the signed URL for
        :param datetime.datetime|datetime.timedelta expiration: the datetime for the URL to expire at or the amount of time after which it should expire
        :return str:
        """
        if os.environ.get("STORAGE_EMULATOR_HOST"):
            api_access_endpoint = {"api_access_endpoint": os.environ["STORAGE_EMULATOR_HOST"]}
        else:
            api_access_endpoint = {}

        blob = self._blob(cloud_path)

        try:
            # Use compute engine credentials if running on e.g. Google Cloud Run, performing a refresh request to get
            # the access token of the credentials (otherwise it's `None`).
            credentials, _ = google.auth.default()
            request = google_requests.Request()
            credentials.refresh(request)

            signing_credentials = compute_engine.IDTokenCredentials(
                request,
                "",
                service_account_email=credentials.service_account_email,
            )

            return blob.generate_signed_url(
                expiration=expiration,
                credentials=signing_credentials,
                version="v4",
                **api_access_endpoint,
            )

        except google.auth.exceptions.RefreshError:
            # Use local service account key.
            return blob.generate_signed_url(expiration=expiration, **api_access_endpoint)

    def _get_bucket_and_path_in_bucket(self, cloud_path):
        """Get the bucket and path within the bucket from the given cloud path.

        :param str cloud_path: the path to get the bucket and path within the bucket from
        :return (google.cloud.storage.bucket.Bucket, str): the bucket and path within the bucket
        """
        bucket_name, path_in_bucket = split_bucket_name_from_cloud_path(cloud_path)

        try:
            bucket = self.client.get_bucket(bucket_or_name=bucket_name)
        except google.api_core.exceptions.NotFound:
            raise CloudStorageBucketNotFound(f"The bucket {bucket_name!r} was not found.") from None

        return bucket, path_in_bucket

    def _blob(self, cloud_path=None):
        """Instantiate a blob for the given bucket at the given path. Note that this is not synced up with Google Cloud.

        :param str|None cloud_path:
        :raise octue.exceptions.CloudStorageBucketNotFound: if the bucket isn't found
        :return google.cloud.storage.blob.Blob:
        """
        bucket, path_in_bucket = self._get_bucket_and_path_in_bucket(cloud_path)
        return bucket.blob(blob_name=self._strip_leading_slash(path_in_bucket))

    def _strip_leading_slash(self, path):
        """Strip the leading slash from a path.

        :param str path:
        :return str:
        """
        return path.lstrip("/")

    def _compute_crc32c_checksum(self, string_or_bytes):
        """Compute the CRC32 checksum of the string.

        :param str|bytes string_or_bytes:
        :return str:
        """
        if isinstance(string_or_bytes, str):
            string_or_bytes = string_or_bytes.encode()

        checksum = Checksum(string_or_bytes)
        return base64.b64encode(checksum.digest()).decode("utf-8")

    def _encode_metadata(self, metadata):
        """Encode metadata as a dictionary of JSON strings.

        :param dict metadata:
        :return dict:
        """
        if not isinstance(metadata, dict):
            raise TypeError(f"Metadata for Google Cloud storage should be a dictionary; received {metadata!r}")

        return {key: json.dumps(value, cls=OctueJSONEncoder) for key, value in metadata.items()}
