import copy
import datetime
import functools
import json
import logging
import os
import tempfile
from urllib.parse import urlparse

import google.api_core.exceptions
import pkg_resources
from google_crc32c import Checksum


try:
    import h5py
except ModuleNotFoundError:
    pass

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import CloudLocationNotSpecified, FileNotFoundException, InvalidInputException
from octue.migrations.cloud_storage import translate_bucket_name_and_path_in_bucket_to_cloud_path
from octue.mixins import Filterable, Hashable, Identifiable, Labelable, Pathable, Serialisable, Taggable
from octue.mixins.hashable import EMPTY_STRING_HASH_VALUE
from octue.utils import isfile
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.local_metadata import LOCAL_METADATA_FILENAME, load_local_metadata_file


logger = logging.getLogger(__name__)

OCTUE_METADATA_NAMESPACE = "octue"

ID_DEFAULT = None
TAGS_DEFAULT = None
LABELS_DEFAULT = None


class Datafile(Labelable, Taggable, Serialisable, Pathable, Identifiable, Hashable, Filterable):
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

    :param str|None path: The path of this file locally or in the cloud, which may include folders or subfolders, within the dataset. If no path_from parameter is set, then absolute paths are acceptable, otherwise relative paths are required.
    :param str|None local_path: If a cloud path is given as the `path` parameter, this is the path to an existing local file that is known to be in sync with the cloud object
    :param str|None cloud_path: If a local path is given for the `path` parameter, this is a cloud path to keep in sync with the local file
    :param datetime.datetime|int|float|None timestamp: A posix timestamp associated with the file, in seconds since epoch, typically when it was created but could relate to a relevant time point for the data
    :param str id: The Universally Unique ID of this file (checked to be valid if not None, generated if None)
    :param Pathable path_from: The root Pathable object (typically a Dataset) that this Datafile's path is relative to.
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
        "cloud_path",
        "tags",
        "labels",
        "timestamp",
        "_cloud_metadata",
    )

    def __init__(
        self,
        path,
        local_path=None,
        cloud_path=None,
        timestamp=None,
        id=ID_DEFAULT,
        path_from=None,
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
            tags=tags,
            labels=labels,
            path=path,
            path_from=path_from,
        )

        self.timestamp = timestamp
        self.extension = os.path.splitext(path)[-1].strip(".")

        self._local_path = None
        self._cloud_path = None
        self._hypothetical = hypothetical
        self._open_attributes = {"mode": mode, "update_cloud_metadata": update_cloud_metadata, **kwargs}
        self._cloud_metadata = {}

        if storage.path.is_qualified_cloud_path(self.path):
            self._cloud_path = path

            if not self._hypothetical:
                # Collect any non-`None` metadata instantiation parameters so the user can be warned if they conflict
                # with any metadata already on the cloud object.
                initialisation_parameters = {}

                for parameter in ("id", "timestamp", "tags", "labels"):
                    value = locals().get(parameter)
                    if value is not None:
                        initialisation_parameters[parameter] = value

                self._use_cloud_metadata(**initialisation_parameters)

            if local_path:
                # If there is no file at the given local path or the file is different to the one in the cloud, download
                # the cloud file locally.
                if not os.path.exists(local_path) or self._cloud_metadata.get("crc32c") != calculate_hash(local_path):
                    self.download(local_path)
                else:
                    self._local_path = local_path

        else:
            self._local_path = self.absolute_path
            self._get_local_metadata()

            # Run integrity checks on the file.
            if not skip_checks:
                self.check(**kwargs)

            if cloud_path:
                self.cloud_path = cloud_path

    @classmethod
    def deserialise(cls, serialised_datafile, path_from=None):
        """Deserialise a Datafile from a dictionary. The `path_from` parameter is only used if the path in the
        serialised Dataset is relative.

        :param dict serialised_datafile:
        :param octue.mixins.Pathable path_from:
        :return Datafile:
        """
        serialised_datafile = copy.deepcopy(serialised_datafile)
        cloud_metadata = serialised_datafile.pop("_cloud_metadata", {})

        if not os.path.isabs(serialised_datafile["path"]) and not storage.path.is_qualified_cloud_path(
            serialised_datafile["path"]
        ):
            datafile = Datafile(**serialised_datafile, path_from=path_from)
        else:
            datafile = Datafile(**serialised_datafile)

        datafile._cloud_metadata = cloud_metadata
        return datafile

    @property
    def name(self):
        """Get the name of the datafile.

        :return str:
        """
        return self._name or str(os.path.split(self.path)[-1])

    @property
    def cloud_path(self):
        """Get the cloud path of the datafile.

        :return str|None:
        """
        return self._cloud_path

    @cloud_path.setter
    def cloud_path(self, path):
        """Set the cloud path of the datafile.

        :param str|None path:
        :return None:
        """
        if path is None:

            if not self.exists_locally:
                raise CloudLocationNotSpecified(
                    "The cloud path cannot be reset because this datafile only exists in the cloud."
                )

            self._cloud_path = None

        else:
            self.to_cloud(cloud_path=path)

    @property
    def cloud_protocol(self):
        """Get the cloud protocol of the datafile if it exists in the cloud (e.g. "gs" for a cloud path of
        "gs://my-bucket/my-file.txt").

        :return str|None:
        """
        if not self.exists_in_cloud:
            return None
        return urlparse(self.cloud_path).scheme

    @property
    def bucket_name(self):
        """Get the name of the bucket the datafile exists in if it exists in the cloud.

        :return str|None:
        """
        if self.cloud_path:
            return storage.path.split_bucket_name_from_gs_path(self.cloud_path)[0]
        return None

    @property
    def path_in_bucket(self):
        """Get the path of the datafile in its bucket if it exists in the cloud.

        :return str|None:
        """
        if self.cloud_path:
            return storage.path.split_bucket_name_from_gs_path(self.cloud_path)[1]
        return None

    @property
    def timestamp(self):
        """Get the timestamp of the datafile.

        :return float:
        """
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
        """Get the size of the datafile in bytes.

        :return float:
        """
        if self._path_is_in_google_cloud_storage:
            return self._cloud_metadata.get("size")

        return os.path.getsize(self.absolute_path)

    @property
    def exists_in_cloud(self):
        """Return `True` if the file exists in the cloud.

        :return bool:
        """
        return self.cloud_path is not None

    @property
    def exists_locally(self):
        """Return `True` if the file exists locally.

        :return bool:
        """
        return self._local_path is not None

    @property
    def local_path(self):
        """Get the local path for the datafile, downloading it from the cloud to a temporary file if necessary. If
        downloaded, the local path is added to a cache to avoid downloading again in the same runtime.
        """
        if self._local_path:
            return self._local_path

        return self.download()

    @local_path.setter
    def local_path(self, path):
        """Set the local path of the datafile to an empty path (a path not corresponding to an existing file) and
        download the contents of the corresponding cloud file to the local path.

        :param str path:
        :raise FileExistsError: if the path corresponds to an existing file
        :return None:
        """
        if path is None:
            if not self.exists_in_cloud:
                raise CloudLocationNotSpecified(
                    "The local path cannot be reset because this datafile only exists locally."
                )

            self._local_path = None
            return

        if os.path.exists(path):
            raise FileExistsError(
                "Only a path not corresponding to an existing file can be used. This is because the contents of the "
                "existing cloud file will be downloaded to the new local path and would overwrite any existing file at "
                "the given path."
            )

        if self.exists_in_cloud:
            GoogleCloudStorageClient().download_to_file(local_path=path, cloud_path=self.cloud_path)

        self._local_path = os.path.abspath(path)

    @property
    def open(self):
        """Get a context manager for handling the opening and closing of the datafile for reading/editing.

        :return type: the class octue.resources.datafile._DatafileContextManager
        """
        return functools.partial(_DatafileContextManager, self)

    @property
    def _local_metadata_path(self):
        """Get the path to the datafile's local metadata file (if the datafile exists locally).

        :return str|None:
        """
        if not self.exists_locally:
            return None

        return os.path.join(os.path.dirname(self._local_path), LOCAL_METADATA_FILENAME)

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

    def to_cloud(self, cloud_path=None, bucket_name=None, path_in_bucket=None, update_cloud_metadata=True):
        """Upload a datafile to Google Cloud Storage.

        :param str|None cloud_path: full path to cloud storage location to store datafile at (e.g. `gs://bucket_name/path/to/file.csv`)
        :param bool update_cloud_metadata: if `True`, update the metadata of the datafile in the cloud at upload time
        :return str: gs:// path for datafile
        """
        if bucket_name:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_in_bucket)

        cloud_path = self._get_cloud_location(cloud_path)

        self._get_cloud_metadata()

        # If the datafile's file has been changed locally, overwrite its cloud copy.
        if self._cloud_metadata.get("crc32c") != self.hash_value:
            GoogleCloudStorageClient().upload_file(
                local_path=self.local_path,
                cloud_path=cloud_path,
                metadata=self.metadata(),
            )

        if update_cloud_metadata:
            # If the datafile's metadata has been changed locally, update the cloud file's metadata.
            local_metadata = self.metadata()

            if self._cloud_metadata.get("custom_metadata") != local_metadata:
                self._update_cloud_metadata()

        return self.cloud_path

    def download(self, local_path=None):
        """Download the file from the cloud to the given local path or a random temporary path.

        :param str|None local_path:
        :raise CloudLocationNotSpecified: if the datafile does not exist in the cloud
        :return str: path to local file
        """
        if not self.exists_in_cloud:
            raise CloudLocationNotSpecified("Cannot download a file that doesn't exist in the cloud.")

        # Avoid downloading to a local path if the datafile has already been downloaded to it.
        if (local_path is None and self._local_path is not None) or (
            local_path is not None and local_path == self._local_path
        ):
            return self._local_path

        if local_path is not None:
            self._local_path = os.path.abspath(local_path)
        else:
            self._local_path = tempfile.NamedTemporaryFile(delete=False).name

        try:
            GoogleCloudStorageClient().download_to_file(local_path=self._local_path, cloud_path=self.cloud_path)

        except google.api_core.exceptions.NotFound as e:
            # If in reading mode, raise an error if no file exists at the path; if in a writing mode, create a new file.
            if self._open_attributes["mode"] == "r":
                raise e

        # Now use hash value of local file instead of cloud file.
        self.reset_hash()
        return self._local_path

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
        """Return `True` if the datafile exists on the current system.

        :return bool:
        """
        return isfile(self.absolute_path)

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

    def _get_cloud_metadata(self):
        """Get the cloud metadata for the datafile.

        :return dict:
        """
        if not self.cloud_path:
            self._raise_cloud_location_error()

        cloud_metadata = GoogleCloudStorageClient().get_metadata(cloud_path=self.cloud_path)

        if cloud_metadata:
            self._cloud_metadata = cloud_metadata

    def _update_cloud_metadata(self):
        """Update the cloud metadata for the datafile.

        :return None:
        """
        if not self.cloud_path:
            self._raise_cloud_location_error()

        GoogleCloudStorageClient().overwrite_custom_metadata(metadata=self.metadata(), cloud_path=self.cloud_path)

    def _use_cloud_metadata(self, **initialisation_parameters):
        """Populate the datafile's attributes from the metadata of the cloud object located at its path (by necessity a
        cloud path) and project name. If there is a conflict between the cloud metadata and a given local initialisation
        parameter, the local value is used.

        :param initialisation_parameters: key-value pairs of initialisation parameter names and values (provide to check for conflicts with cloud metadata)
        :return None:
        """
        self._get_cloud_metadata()
        cloud_custom_metadata = self._cloud_metadata.get("custom_metadata", {})
        self._warn_about_attribute_conflicts(cloud_custom_metadata, **initialisation_parameters)

        self._set_id(
            initialisation_parameters.get(
                "id", cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__id", ID_DEFAULT)
            )
        )

        self.immutable_hash_value = self._cloud_metadata.get("crc32c", EMPTY_STRING_HASH_VALUE)

        for attribute in ("timestamp", "tags", "labels"):
            setattr(
                self,
                attribute,
                initialisation_parameters.get(
                    attribute, cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__{attribute}")
                ),
            )

    def _get_local_metadata(self):
        """Get the datafile's local metadata from the local metadata records file and apply it to the datafile instance.
        If no metadata is stored for the datafile, do nothing.

        :return None:
        """
        existing_metadata_records = load_local_metadata_file(self._local_metadata_path)
        datafile_metadata = existing_metadata_records.get("datafiles", {}).get(self.name, {})

        if not datafile_metadata:
            return

        if "id" in datafile_metadata:
            self._set_id(datafile_metadata["id"])

        for parameter in ("timestamp", "tags", "labels"):
            if parameter in datafile_metadata:
                setattr(self, parameter, datafile_metadata[parameter])

    def _update_local_metadata(self):
        """Create or update the local octue metadata file with the datafile's metadata.

        :return None:
        """
        existing_metadata_records = load_local_metadata_file(self._local_metadata_path)

        if not existing_metadata_records.get("datafiles"):
            existing_metadata_records["datafiles"] = {}

        existing_metadata_records["datafiles"][self.name] = self.metadata(use_octue_namespace=False)

        with open(self._local_metadata_path, "w") as f:
            json.dump(existing_metadata_records, f, cls=OctueJSONEncoder)

    def _warn_about_attribute_conflicts(self, cloud_custom_metadata, **initialisation_parameters):
        """Raise a warning if there is a conflict between the cloud custom metadata and the given initialisation
        parameters if the cloud value is not `None` or an empty collection.

        :param dict cloud_custom_metadata:
        :return None:
        """
        for attribute_name, attribute_value in initialisation_parameters.items():

            cloud_metadata_value = cloud_custom_metadata.get(f"{OCTUE_METADATA_NAMESPACE}__{attribute_name}")

            if cloud_metadata_value == attribute_value or not cloud_metadata_value:
                continue

            logger.warning(
                f"The value {cloud_metadata_value!r} of the {type(self).__name__} attribute {attribute_name!r} from "
                f"the cloud conflicts with the value given locally at instantiation {attribute_value!r}. The local "
                f"value has been used and will overwrite the cloud value if the datafile is saved."
            )

    def _calculate_hash(self):
        """Calculate the hash of the file."""
        try:
            hash = calculate_hash(self.local_path)
            return super()._calculate_hash(hash)
        except FileNotFoundError:
            return self._cloud_metadata.get("crc32c", EMPTY_STRING_HASH_VALUE)

    def _get_cloud_location(self, cloud_path=None):
        """Get the cloud location details for the bucket, allowing the keyword arguments to override any stored values.
        Once the cloud location details have been determined, update the stored cloud location details.

        :param str|None cloud_path:
        :raise octue.exceptions.CloudLocationNotSpecified: if an exact cloud location isn't provided and isn't available implicitly (i.e. the Datafile wasn't loaded from the cloud previously)
        :return (str, str): project name and cloud path
        """
        cloud_path = cloud_path or self.cloud_path

        if not cloud_path:
            self._raise_cloud_location_error()

        self._cloud_path = cloud_path
        return cloud_path

    def _raise_cloud_location_error(self):
        """Raise an error indicating that the cloud location of the datafile has not yet been specified.

        :raise CloudLocationNotSpecified:
        :return None:
        """
        raise CloudLocationNotSpecified(
            f"{self!r} wasn't previously loaded from the cloud so doesn't have an implicit cloud location - please "
            f"specify its exact location (its project name and cloud path)."
        )


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
    :param str mode: open the datafile for reading/editing in this mode (the mode options are the same as for the builtin `open` function)
    :param bool update_cloud_metadata: if this is True, update the cloud metadata of the datafile when the context is exited
    :return None:
    """

    MODIFICATION_MODES = {"w", "a", "x", "+", "U"}

    def __init__(self, datafile, mode="r", update_cloud_metadata=True, **kwargs):
        self.datafile = datafile
        self.mode = mode
        self._update_cloud_metadata = update_cloud_metadata
        self.kwargs = kwargs
        self._fp = None

    def __enter__(self):
        """Open the datafile, first downloading it from the cloud if necessary.

        :return io.TextIOWrapper:
        """
        if "w" in self.mode:
            os.makedirs(os.path.split(self.datafile.local_path)[0], exist_ok=True)

        if self.datafile.extension == "hdf5":
            try:
                pkg_resources.get_distribution("h5py")
                self._fp = h5py.File(self.datafile.local_path, self.mode, **self.kwargs)
            except pkg_resources.DistributionNotFound:
                raise ImportError(
                    "To use datafiles with HDF5 files, please install octue with the 'hdf5' option i.e. "
                    "`pip install octue[hdf5]`."
                )
        else:
            self._fp = open(self.datafile.local_path, self.mode, **self.kwargs)

        return self._fp

    def __exit__(self, *args):
        """Close the datafile, updating the corresponding file in the cloud if necessary and its metadata if
        `self._update_cloud_metadata` is True.

        :return None:
        """
        if self._fp is not None:
            self._fp.close()

        if any(character in self.mode for character in self.MODIFICATION_MODES):

            # If the datafile is local-first, update its local metadata.
            if not storage.path.is_qualified_cloud_path(self.datafile.path):
                self.datafile._update_local_metadata()

            if self.datafile.exists_in_cloud:
                self.datafile.to_cloud(update_cloud_metadata=self._update_cloud_metadata)


def calculate_hash(path):
    """Calculate the hash of the file at the given path."""
    hash = Checksum()

    with open(path, "rb") as f:
        # Read and update hash value in blocks of 4K.
        for byte_block in iter(lambda: f.read(4096), b""):
            hash.update(byte_block)

    return hash
