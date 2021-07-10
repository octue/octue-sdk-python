import datetime
import functools
import logging
import os
import tempfile
import google.api_core.exceptions
import pkg_resources
from google_crc32c import Checksum

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.cloud.storage.path import CLOUD_STORAGE_PROTOCOL
from octue.exceptions import CloudLocationNotSpecified, FileNotFoundException, InvalidInputException
from octue.mixins import Filterable, Hashable, Identifiable, Labelable, Loggable, Pathable, Serialisable, Taggable
from octue.mixins.hashable import EMPTY_STRING_HASH_VALUE
from octue.utils import isfile


module_logger = logging.getLogger(__name__)


TEMPORARY_LOCAL_FILE_CACHE = {}
OCTUE_METADATA_NAMESPACE = "octue"

ID_DEFAULT = None
TAGS_DEFAULT = None
LABELS_DEFAULT = None


class Datafile(Labelable, Taggable, Serialisable, Pathable, Loggable, Identifiable, Hashable, Filterable):
    """A representation of a data file on the Octue system. If the given path is a cloud path and `hypothetical` is not
    `True`, the datafile's metadata is pulled from the given cloud location, and any conflicting parameters (see the
    `Datafile.metadata` method description for the parameter names concerned) are ignored. The metadata of cloud
    datafiles can be changed using the `Datafile.update_metadata` method, but not during instantiation.

    Files in a manifest look like this:

        {
          "path": "folder/subfolder/file_1.csv",
          "extension": "csv",
          "tags": {},
          "labels": [],
          "timestamp": datetime.datetime(2021, 5, 3, 18, 15, 58, 298086),
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        },

    :param datetime.datetime|int|float|None timestamp: A posix timestamp associated with the file, in seconds since epoch, typically when it was created but could relate to a relevant time point for the data
    :param str id: The Universally Unique ID of this file (checked to be valid if not None, generated if None)
    :param logging.Logger logger: A logger instance to which operations with this datafile will be logged. Defaults to the module logger.
    :param Union[str, path-like] path: The path of this file, which may include folders or subfolders, within the dataset. If no path_from parameter is set, then absolute paths are acceptable, otherwise relative paths are required.
    :param Pathable path_from: The root Pathable object (typically a Dataset) that this Datafile's path is relative to.
    :param str|None project_name: The name of the cloud project if the datafile is located in the cloud
    :param dict|TagDict tags: key-value pairs with string keys conforming to the Octue tag format (see TagDict)
    :param iter(str) labels: Space-separated string of labels relevant to this file
    :param bool skip_checks:
    :param str mode: if using as a context manager, open the datafile for reading/editing in this mode (the mode options are the same as for the builtin open function)
    :param bool update_cloud_metadata: if using as a context manager and this is True, update the cloud metadata of the datafile when the context is exited
    :param bool hypothetical: True if the file does not actually exist or access is not available at instantiation
    :return None:
    """

    _SERIALISE_FIELDS = (
        "id",
        "name",
        "path",
        "tags",
        "labels",
        "timestamp",
        "_cloud_metadata",
    )

    def __init__(
        self,
        path,
        timestamp=None,
        id=ID_DEFAULT,
        logger=None,
        path_from=None,
        project_name=None,
        tags=TAGS_DEFAULT,
        labels=LABELS_DEFAULT,
        skip_checks=True,
        mode="r",
        update_cloud_metadata=True,
        hypothetical=False,
        **kwargs,
    ):
        super().__init__(
            id=id,
            name=kwargs.pop("name", None),
            immutable_hash_value=kwargs.pop("immutable_hash_value", None),
            logger=logger,
            tags=tags,
            labels=labels,
            path=path,
            path_from=path_from,
        )

        self.timestamp = timestamp
        self.extension = self._get_extension_from_path()
        self._hypothetical = hypothetical
        self._open_attributes = {"mode": mode, "update_cloud_metadata": update_cloud_metadata, **kwargs}
        self._cloud_metadata = {"project_name": project_name}

        if self.is_in_cloud and not self._hypothetical:
            self._use_cloud_metadata(id=id, timestamp=timestamp, tags=tags, labels=labels)
            return

        # Run integrity checks on the file
        if not skip_checks:
            self.check(**kwargs)

    def __enter__(self):
        self._open_context_manager = self.open(**self._open_attributes)
        return self, self._open_context_manager.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._open_context_manager.__exit__(exc_type, exc_val, exc_tb)

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
    def deserialise(cls, serialised_datafile, path_from=None):
        """Deserialise a Datafile from a dictionary. The `path_from` parameter is only used if the path in the
        serialised Dataset is relative.

        :param dict serialised_datafile:
        :param octue.mixins.Pathable path_from:
        :return Datafile:
        """
        cloud_metadata = serialised_datafile.pop("_cloud_metadata", {})

        if not os.path.isabs(serialised_datafile["path"]) and not serialised_datafile["path"].startswith(
            CLOUD_STORAGE_PROTOCOL
        ):
            datafile = Datafile(**serialised_datafile, path_from=path_from)
        else:
            datafile = Datafile(**serialised_datafile)

        datafile._cloud_metadata = cloud_metadata
        return datafile

    def to_cloud(
        self, project_name=None, cloud_path=None, bucket_name=None, path_in_bucket=None, update_cloud_metadata=True
    ):
        """Upload a datafile to Google Cloud Storage. Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be
        provided.

        :param str|None project_name: name of Google Cloud project to store datafile in
        :param str|None cloud_path: full path to cloud storage location to store datafile at (e.g. `gs://bucket_name/path/to/file.csv`)
        :param str|None bucket_name: name of bucket to store datafile in
        :param str|None path_in_bucket: cloud storage path to store datafile at (e.g. `path/to/file.csv`)
        :param bool update_cloud_metadata: if `True`, update the metadata of the datafile in the cloud at upload time
        :return str: gs:// path for datafile
        """
        project_name, bucket_name, path_in_bucket = self._get_cloud_location(
            project_name, cloud_path, bucket_name, path_in_bucket
        )

        self.get_cloud_metadata(project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket)

        # If the datafile's file has been changed locally, overwrite its cloud copy.
        if self._cloud_metadata.get("crc32c") != self.hash_value:
            GoogleCloudStorageClient(project_name=project_name).upload_file(
                local_path=self.get_local_path(),
                bucket_name=bucket_name,
                path_in_bucket=path_in_bucket,
                metadata=self.metadata(),
            )

        if update_cloud_metadata:
            # If the datafile's metadata has been changed locally, update the cloud file's metadata.
            local_metadata = self.metadata()

            if self._cloud_metadata.get("custom_metadata") != local_metadata:
                self.update_cloud_metadata(project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket)

        return cloud_path or storage.path.generate_gs_path(bucket_name, path_in_bucket)

    def get_cloud_metadata(self, project_name=None, cloud_path=None, bucket_name=None, path_in_bucket=None):
        """Get the cloud metadata for the datafile. Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be
        provided.

        :param str|None project_name:
        :param str|None cloud_path:
        :param str|None bucket_name:
        :param str|None path_in_bucket:
        :return dict:
        """
        project_name, bucket_name, path_in_bucket = self._get_cloud_location(
            project_name, cloud_path, bucket_name, path_in_bucket
        )

        cloud_metadata = GoogleCloudStorageClient(project_name).get_metadata(
            bucket_name=bucket_name, path_in_bucket=path_in_bucket
        )

        if cloud_metadata is None:
            return None

        self._cloud_metadata = cloud_metadata

    def update_cloud_metadata(self, project_name=None, cloud_path=None, bucket_name=None, path_in_bucket=None):
        """Update the cloud metadata for the datafile. Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be
        provided.

        :param str|None project_name:
        :param str|None cloud_path:
        :param str|None bucket_name:
        :param str|None path_in_bucket:
        :return None:
        """
        project_name, bucket_name, path_in_bucket = self._get_cloud_location(
            project_name, cloud_path, bucket_name, path_in_bucket
        )

        GoogleCloudStorageClient(project_name=project_name).overwrite_custom_metadata(
            metadata=self.metadata(),
            bucket_name=bucket_name,
            path_in_bucket=path_in_bucket,
        )

    @property
    def name(self):
        return self._name or str(os.path.split(self.path)[-1])

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        """Set the datafile's timestamp.

        :param datetime.datetime|int|float|None value:
        :raise TypeError: if value is of an incorrect type
        :return None:
        """
        if isinstance(value, datetime.datetime) or value is None:
            self._timestamp = value
        elif isinstance(value, (int, float)):
            self._timestamp = datetime.datetime.fromtimestamp(value)
        else:
            raise TypeError(
                f"timestamp should be a datetime.datetime instance, an int, a float, or None; received {value!r}"
            )

    @property
    def posix_timestamp(self):
        """Get the timestamp of the datafile in posix format.

        :return float:
        """
        if self.timestamp is None:
            return None

        return self.timestamp.timestamp()

    @property
    def _last_modified(self):
        """Get the date/time the file was last modified in units of seconds since epoch (posix time).

        :return float:
        """
        if self._path_is_in_google_cloud_storage:
            last_modified = self._cloud_metadata.get("updated")

            if last_modified is None:
                return None

            return last_modified.timestamp()

        return os.path.getmtime(self.absolute_path)

    @property
    def size_bytes(self):
        if self._path_is_in_google_cloud_storage:
            return self._cloud_metadata.get("size")

        return os.path.getsize(self.absolute_path)

    @property
    def is_in_cloud(self):
        """Does the file exist in the cloud?

        :return bool:
        """
        return self.path.startswith(CLOUD_STORAGE_PROTOCOL)

    def get_local_path(self):
        """Get the local path for the datafile, downloading it from the cloud to a temporary file if necessary. If
        downloaded, the local path is added to a cache to avoid downloading again in the same runtime.

        :raise octue.exceptions.FileLocationError: if the file is not located in the cloud (i.e. it is local)
        :return str:
        """
        if not self.is_in_cloud:
            return self.absolute_path

        if self.absolute_path in TEMPORARY_LOCAL_FILE_CACHE:
            return TEMPORARY_LOCAL_FILE_CACHE[self.absolute_path]

        temporary_local_path = tempfile.NamedTemporaryFile(delete=False).name

        try:
            GoogleCloudStorageClient(project_name=self._cloud_metadata["project_name"]).download_to_file(
                local_path=temporary_local_path, cloud_path=self.absolute_path
            )

        except google.api_core.exceptions.NotFound as e:
            # If in reading mode, raise an error if no file exists at the path; if in a writing mode, create a new file.
            if self._open_attributes["mode"] == "r":
                raise e

        TEMPORARY_LOCAL_FILE_CACHE[self.absolute_path] = temporary_local_path

        # Now use hash value of local file instead of cloud file.
        self.reset_hash()
        return temporary_local_path

    def clear_from_file_cache(self):
        """Clear the datafile from the temporary local file cache, if it is in there. If datafile.get_local_path is
        called again and the datafile is a cloud datafile, the file will be re-downloaded to a new temporary local path,
        allowing any independent cloud updates to be synced locally.

        :return None:
        """
        if self.absolute_path in TEMPORARY_LOCAL_FILE_CACHE:
            del TEMPORARY_LOCAL_FILE_CACHE[self.absolute_path]

    def _use_cloud_metadata(self, **initialisation_parameters):
        """Populate the datafile's attributes from the cloud location defined by its path (by necessity a cloud path)
        and project name.

        :param initialisation_parameters: key-value pairs of initialisation parameter names and values (provide to check for conflicts with cloud metadata)
        :return None:
        """
        self.get_cloud_metadata(project_name=self._cloud_metadata["project_name"], cloud_path=self.path)
        cloud_custom_metadata = self._cloud_metadata.get("custom_metadata", {})
        self._check_for_attribute_conflict(cloud_custom_metadata, **initialisation_parameters)

        self._set_id(cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__id", ID_DEFAULT))
        self.immutable_hash_value = self._cloud_metadata.get("crc32c", EMPTY_STRING_HASH_VALUE)
        self.timestamp = cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__timestamp")
        self.tags = cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__tags", TAGS_DEFAULT)
        self.labels = cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__labels", LABELS_DEFAULT)

    def _check_for_attribute_conflict(self, cloud_custom_metadata, **initialisation_parameters):
        """Raise a warning if there is a conflict between the cloud custom metadata and the given initialisation
        parameters if the initialisation parameters are not `None`.

        :param dict cloud_custom_metadata:
        :return None:
        """
        for attribute_name, attribute_value in initialisation_parameters.items():

            if attribute_value is None:
                continue

            cloud_metadata_value = cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__{attribute_name}")

            if cloud_metadata_value == attribute_value:
                continue

            module_logger.warning(
                f"The value {cloud_metadata_value!r} of the {type(self).__name__} attribute {attribute_name!r} from "
                f"the cloud conflicts with the value given locally at instantiation {attribute_value!r}. This may not "
                f"be a problem, but note that cloud datafile metadata cannot be changed at instantiation. The cloud "
                f"value has been used."
            )

    def _get_extension_from_path(self, path=None):
        """Gets extension of a file, either from a provided file path or from self.path field"""
        path = path or self.path
        return os.path.splitext(path)[-1].strip(".")

    def _calculate_hash(self):
        """Calculate the hash of the file."""
        hash = Checksum()

        with open(self.get_local_path(), "rb") as f:
            # Read and update hash value in blocks of 4K.
            for byte_block in iter(lambda: f.read(4096), b""):
                hash.update(byte_block)

        return super()._calculate_hash(hash)

    def _get_cloud_location(self, project_name=None, cloud_path=None, bucket_name=None, path_in_bucket=None):
        """Get the cloud location details for the bucket, allowing the keyword arguments to override any stored values.
        Either (`bucket_name` and `path_in_bucket`) or `cloud_path` must be provided. Once the cloud location details
        have been determined, update the stored cloud location details.

        :param str|None project_name:
        :param str|None cloud_path:
        :param str|None bucket_name:
        :param str|None path_in_bucket:
        :raise octue.exceptions.CloudLocationNotSpecified: if an exact cloud location isn't provided and isn't available
            implicitly (i.e. the Datafile wasn't loaded from the cloud previously)
        :return (str, str, str):
        """
        if cloud_path:
            bucket_name, path_in_bucket = storage.path.split_bucket_name_from_gs_path(cloud_path)

        try:
            project_name = project_name or self._cloud_metadata["project_name"]
            bucket_name = bucket_name or self._cloud_metadata["bucket_name"]
            path_in_bucket = path_in_bucket or self._cloud_metadata["path_in_bucket"]
        except KeyError:
            raise CloudLocationNotSpecified(
                f"{self!r} wasn't previously loaded from the cloud so doesn't have an implicit cloud location - please"
                f"specify its exact location (its project_name, bucket_name, and path_in_bucket)."
            )

        self._store_cloud_location(project_name, bucket_name, path_in_bucket)
        return project_name, bucket_name, path_in_bucket

    def _store_cloud_location(self, project_name, bucket_name, path_in_bucket):
        """Store the cloud location of the datafile.

        :param str project_name:
        :param str bucket_name:
        :param str path_in_bucket:
        :return None:
        """
        self._cloud_metadata["project_name"] = project_name
        self._cloud_metadata["bucket_name"] = bucket_name
        self._cloud_metadata["path_in_bucket"] = path_in_bucket

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
        """Get a context manager for handling the opening and closing of the datafile for reading/editing.

        :return type: the class octue.resources.datafile._DatafileContextManager
        """
        return functools.partial(_DatafileContextManager, self)

    def metadata(self, use_octue_namespace=True):
        """Get the datafile's metadata in a serialised form (i.e. the attributes `id`, `timestamp`, `labels`, `tags`,
        and `sdk_version`).

        :param bool use_octue_namespace: if True, prefix metadata names with "octue__"
        :return dict:
        """
        metadata = {
            "id": self.id,
            "timestamp": self.timestamp,
            "tags": self.tags,
            "labels": self.labels,
            "sdk_version": pkg_resources.get_distribution("octue").version,
        }

        if not use_octue_namespace:
            return metadata

        return {f"{OCTUE_METADATA_NAMESPACE}__{key}": value for key, value in metadata.items()}


class _DatafileContextManager:
    """A context manager for opening datafiles for reading and writing locally or from the cloud. Its usage is analogous
    to the builtin open context manager. If opening a local datafile in write mode, the manager will attempt to
    determine if the folder path exists and, if not, will create the folder structure required to write the file.

    Usage:
    ```
    my_datafile = Datafile(path='subfolder/subsubfolder/my_datafile.json)
    with my_datafile.open('w') as fp:
        fp.write("{}")
    ```

    This is equivalent to the standard python:
    ```
    my_datafile = Datafile(path='subfolder/subsubfolder/my_datafile.json)
    os.makedirs(os.path.split(my_datafile.absolute_path)[0], exist_ok=True)
    with open(my_datafile.absolute_path, 'w') as fp:
        fp.write("{}")
    ```

    :param octue.resources.datafile.Datafile datafile:
    :param str mode: open the datafile for reading/editing in this mode (the mode options are the same as for the
        builtin open function)
    :param bool update_cloud_metadata: this is True, update the cloud metadata of
        the datafile when the context is exited
    :return None:
    """

    MODIFICATION_MODES = {"w", "a", "x", "+", "U"}

    def __init__(self, datafile, mode="r", update_cloud_metadata=True, **kwargs):
        self.datafile = datafile
        self.mode = mode
        self._update_cloud_metadata = update_cloud_metadata
        self.kwargs = kwargs
        self._fp = None
        self.path = None

    def __enter__(self):
        """Open the datafile, first downloading it from the cloud if necessary.

        :return io.TextIOWrapper:
        """
        self.path = self.datafile.get_local_path()

        if "w" in self.mode:
            os.makedirs(os.path.split(self.path)[0], exist_ok=True)

        self._fp = open(self.path, self.mode, **self.kwargs)
        return self._fp

    def __exit__(self, *args):
        """Close the datafile, updating the corresponding file in the cloud if necessary and its metadata if
        self._update_cloud_metadata is True.

        :return None:
        """
        if self._fp is not None:
            self._fp.close()

        if self.datafile.is_in_cloud and any(character in self.mode for character in self.MODIFICATION_MODES):
            self.datafile.to_cloud(update_cloud_metadata=self._update_cloud_metadata)
