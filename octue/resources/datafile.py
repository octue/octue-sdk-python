import logging
import os
import tempfile
from datetime import datetime
from google_crc32c import Checksum

from octue import exceptions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.cloud.storage.path import CLOUD_STORAGE_PROTOCOL
from octue.exceptions import FileNotFoundException, InvalidInputException
from octue.mixins import Filterable, Hashable, Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.utils import isfile


module_logger = logging.getLogger(__name__)


FILE_CACHE = {}


ID_DEFAULT = None
CLUSTER_DEFAULT = 0
SEQUENCE_DEFAULT = None
TAGS_DEFAULT = None


class Datafile(Taggable, Serialisable, Pathable, Loggable, Identifiable, Hashable, Filterable):
    """Class for representing data files on the Octue system.

    Files in a manifest look like this:

        {
          "path": "folder/subfolder/file_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "tags": "",
          "timestamp": 0,
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        },

    :parameter float|None timestamp: A posix timestamp associated with the file, in seconds since epoch, typically when
        it was created but could relate to a relevant time point for the data
    :param str id: The Universally Unique ID of this file (checked to be valid if not None, generated if None)
    :param logging.Logger logger: A logger instance to which operations with this datafile will be logged. Defaults to
        the module logger.
    :param Union[str, path-like] path: The path of this file, which may include folders or subfolders, within the
        dataset. If no path_from parameter is set, then absolute paths are acceptable, otherwise relative paths are
        required.
    :param Pathable path_from: The root Pathable object (typically a Dataset) that this Datafile's path is relative to.
    :param int cluster: The cluster of files, within a dataset, to which this belongs (default 0)
    :param int sequence: A sequence number of this file within its cluster (if sequences are appropriate)
    :param str tags: Space-separated string of tags relevant to this file
    :param bool skip_checks:

    :return None:
    """

    _ATTRIBUTES_TO_HASH = "name", "cluster", "sequence", "timestamp", "tags"
    _EXCLUDE_SERIALISE_FIELDS = ("logger", "open")

    def __init__(
        self,
        timestamp,
        id=ID_DEFAULT,
        logger=None,
        path=None,
        path_from=None,
        cluster=CLUSTER_DEFAULT,
        sequence=SEQUENCE_DEFAULT,
        tags=TAGS_DEFAULT,
        skip_checks=True,
        **kwargs,
    ):
        super().__init__(
            id=id, hash_value=kwargs.get("hash_value"), logger=logger, tags=tags, path=path, path_from=path_from
        )

        self.cluster = cluster
        self.sequence = sequence
        self.timestamp = timestamp

        if path is None:
            raise InvalidInputException("You must supply a valid 'path' for a Datafile")

        # Set up the file extension or get it from the file path if none passed
        self.extension = self._get_extension_from_path()

        # Run integrity checks on the file
        if not skip_checks:
            self.check(**kwargs)

        self._gcp_metadata = {}

    def __lt__(self, other):
        if not isinstance(other, Datafile):
            raise TypeError(f"An object of type {type(self)} cannot be compared with {type(other)}.")
        return self.absolute_path < other.absolute_path

    def __gt__(self, other):
        if not isinstance(other, Datafile):
            raise TypeError(f"An object of type {type(self)} cannot be compared with {type(other)}.")
        return self.absolute_path > other.absolute_path

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    @classmethod
    def from_cloud(cls, project_name, bucket_name, datafile_path, timestamp=None):
        """Instantiate a Datafile from a previously-persisted Datafile in Google Cloud storage. To instantiate a
        Datafile from a regular file on Google Cloud storage, the usage is the same, but include a meaningful value for
        the `timestamp` parameter.

        :param str project_name:
        :param str bucket_name:
        :param str datafile_path: path to file represented by datafile
        :param float|None timestamp:
        :return Datafile:
        """
        metadata = GoogleCloudStorageClient(project_name).get_metadata(bucket_name, datafile_path)
        custom_metadata = metadata.get("metadata") or {}

        datafile = cls(
            timestamp=custom_metadata.get("timestamp", timestamp),
            id=custom_metadata.get("id", ID_DEFAULT),
            path=storage.path.generate_gs_path(bucket_name, datafile_path),
            hash_value=custom_metadata.get("hash_value", metadata["crc32c"]),
            cluster=custom_metadata.get("cluster", CLUSTER_DEFAULT),
            sequence=custom_metadata.get("sequence", SEQUENCE_DEFAULT),
            tags=custom_metadata.get("tags", TAGS_DEFAULT),
        )

        datafile._gcp_metadata = metadata
        datafile._gcp_metadata["project_name"] = project_name
        return datafile

    def to_cloud(self, project_name, bucket_name, path_in_bucket):
        """Upload a datafile to Google Cloud Storage.

        :param str project_name:
        :param str bucket_name:
        :param str path_in_bucket:
        :return str: gs:// path for datafile
        """
        GoogleCloudStorageClient(project_name=project_name).upload_file(
            local_path=self.path,
            bucket_name=bucket_name,
            path_in_bucket=path_in_bucket,
            metadata={
                "timestamp": self.timestamp,
                "id": self.id,
                "hash_value": self.hash_value,
                "cluster": self.cluster,
                "sequence": self.sequence,
                "tags": self.tags.serialise(to_string=False),
            },
        )

        return storage.path.generate_gs_path(bucket_name, path_in_bucket)

    @property
    def name(self):
        return str(os.path.split(self.path)[-1])

    @property
    def _last_modified(self):
        """Get the date/time the file was last modified in units of seconds since epoch (posix time)."""
        if self._path_is_in_google_cloud_storage:
            unparsed_datetime = self._gcp_metadata.get("updated")

            if unparsed_datetime is None:
                return unparsed_datetime

            parsed_datetime = datetime.strptime(unparsed_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
            return (parsed_datetime - datetime(1970, 1, 1)).total_seconds()

        return os.path.getmtime(self.absolute_path)

    @property
    def size_bytes(self):
        if self._path_is_in_google_cloud_storage:
            size = self._gcp_metadata.get("size")

            if size is None:
                return size

            return int(size)

        return os.path.getsize(self.absolute_path)

    def is_in_cloud(self):
        """Does the file exist in the cloud?

        :return bool:
        """
        return self.path.startswith(CLOUD_STORAGE_PROTOCOL)

    def download(self):
        """Download the datafile from the cloud.

        :raise octue.exceptions.FileLocationError: if the file is not located in the cloud (i.e. it is local)
        :return str:
        """
        if not self.is_in_cloud():
            raise exceptions.FileLocationError(
                f"Local files cannot be downloaded from the cloud; path: {self.absolute_path}"
            )

        if self.absolute_path in FILE_CACHE:
            return FILE_CACHE[self.absolute_path]

        temporary_location = tempfile.NamedTemporaryFile(delete=False).name

        GoogleCloudStorageClient(project_name=self._gcp_metadata["project_name"]).download_to_file(
            *storage.path.split_bucket_name_from_gs_path(self.absolute_path), local_path=temporary_location
        )

        FILE_CACHE[self.absolute_path] = temporary_location
        return temporary_location

    def _get_extension_from_path(self, path=None):
        """Gets extension of a file, either from a provided file path or from self.path field"""
        path = path or self.path
        return os.path.splitext(path)[-1].strip(".")

    def _calculate_hash(self):
        """Calculate the hash of the file."""
        if self.is_in_cloud():
            return self._gcp_metadata.get("hash_value", "")

        hash = Checksum()

        with open(self.absolute_path, "rb") as f:
            # Read and update hash value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                hash.update(byte_block)

        return super()._calculate_hash(hash)

    def check(self, size_bytes=None, sha=None, last_modified=None, extension=None):
        """Check file presence and integrity"""
        # TODO Check consistency of size_bytes input against self.size_bytes property for a file if we have one
        # TODO Check consistency of sha against file contents if we have a file
        # TODO Check consistency of last_modified date

        if (extension is not None) and not self.path.endswith(extension):
            raise InvalidInputException(
                f"Extension provided ({extension}) does not match file extension (from {self.path}). Pass extension="
                f"None to set extension from filename automatically."
            )

        if not self.exists():
            raise FileNotFoundException(f"No file found at {self.absolute_path}")

    def exists(self):
        """Returns true if the datafile exists on the current system, false otherwise"""
        return isfile(self.absolute_path)

    @property
    def open(self):
        """Context manager to handle the opening and closing of a Datafile.

        If opened in write mode, the manager will attempt to determine if the folder path exists and, if not, will
        create the folder structure required to write the file.

        Use it like:
        ```
        my_datafile = Datafile(timestamp=None, path='subfolder/subsubfolder/my_datafile.json)
        with my_datafile.open('w') as fp:
            fp.write("{}")
        ```
        This is equivalent to the standard python:
        ```
        my_datafile = Datafile(timestamp=None, path='subfolder/subsubfolder/my_datafile.json)
        os.makedirs(os.path.split(my_datafile.absolute_path)[0], exist_ok=True)
        with open(my_datafile.absolute_path, 'w') as fp:
            fp.write("{}")
        ```
        """
        datafile = self

        class DataFileContextManager:
            def __init__(obj, mode="r", **kwargs):
                obj.mode = mode
                obj.kwargs = kwargs
                obj.fp = None

            def __enter__(obj):
                """Open the datafile, first downloading it from the cloud if necessary.

                :return io.TextIOWrapper:
                """
                if datafile.is_in_cloud():
                    path = datafile.download()
                else:
                    path = datafile.absolute_path

                if "w" in obj.mode:
                    os.makedirs(os.path.split(path)[0], exist_ok=True)

                obj.fp = open(path, obj.mode, **obj.kwargs)
                return obj.fp

            def __exit__(obj, *args):
                if obj.fp is not None:
                    obj.fp.close()

        return DataFileContextManager
