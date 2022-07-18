import copy
import datetime
import functools
import json
import logging
import os
import shutil
import tempfile

import google.api_core.exceptions
import pkg_resources
import requests
from google_crc32c import Checksum

from octue.resources.label import LabelSet
from octue.resources.tag import TagDict


# The `h5py` package is only needed if dealing with HDF5 files. It's only available if the `hdf5` extra is provided
# during installation of `octue`.
try:
    import h5py
except (ModuleNotFoundError, ImportError):
    pass

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import CloudLocationNotSpecified, ReadOnlyResource
from octue.mixins import CloudPathable, Filterable, Hashable, Identifiable, Labelable, Metadata, Serialisable, Taggable
from octue.mixins.hashable import EMPTY_STRING_HASH_VALUE
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.metadata import METADATA_FILENAME, load_local_metadata_file, overwrite_local_metadata_file


logger = logging.getLogger(__name__)


OCTUE_METADATA_NAMESPACE = "octue"


class Datafile(Labelable, Taggable, Serialisable, Identifiable, Hashable, Filterable, Metadata, CloudPathable):
    """A representation of a data file on the Octue system. Metadata for the file is obtained from its corresponding
    cloud object or a local `.octue` metadata file, if present. If no stored metadata is available, it can be set during
    or after instantiation.

    :param str|None path: The path of this file locally or in the cloud, which may include folders or subfolders, within the dataset
    :param str|None local_path: If a cloud path is given as the `path` parameter, this is the path to an existing local file that is known to be in sync with the cloud object
    :param str|None cloud_path: If a local path is given for the `path` parameter, this is a cloud path to keep in sync with the local file
    :param datetime.datetime|int|float|None timestamp: A posix timestamp associated with the file, in seconds since epoch, typically when it was created but could relate to a relevant time point for the data
    :param str mode: if using as a context manager, open the datafile for reading/editing in this mode (the mode options are the same as for the builtin `open` function)
    :param bool update_metadata: if using as a context manager and this is `True`, update the stored metadata of the datafile when the context is exited
    :param bool hypothetical: if `True`, ignore any metadata stored for this datafile locally or in the cloud and use whatever is given at instantiation
    :param str id: The Universally Unique ID of this file (checked to be valid if not None, generated if None)
    :param dict|octue.resources.tag.TagDict|None tags: key-value pairs with string keys conforming to the Octue tag format (see `TagDict`)
    :param iter(str)|octue.resources.label.LabelSet|None labels: Space-separated string of labels relevant to this file
    :return None:
    """

    _CLOUD_PATH_ATTRIBUTE_NAME = "cloud_path"

    _METADATA_ATTRIBUTES = ("id", "timestamp", "tags", "labels")

    _SERIALISE_FIELDS = (
        *_METADATA_ATTRIBUTES,
        "name",
        "path",
        "cloud_path",
        "_cloud_metadata",
    )

    def __init__(
        self,
        path,
        local_path=None,
        cloud_path=None,
        timestamp=None,
        mode="r",
        update_metadata=True,
        hypothetical=False,
        id=None,
        tags=None,
        labels=None,
        **kwargs,
    ):
        super().__init__(
            id=id,
            name=kwargs.pop("name", None),
            immutable_hash_value=kwargs.pop("immutable_hash_value", None),
            tags=tags,
            labels=labels,
        )

        self.timestamp = timestamp
        self._open_attributes = {"mode": mode, "update_metadata": update_metadata, **kwargs}
        self._local_path = None
        self._cloud_path = None
        self._cloud_metadata = {}

        if storage.path.is_cloud_path(path):
            self._instantiate_from_cloud_object(path, local_path, ignore_stored_metadata=hypothetical)
        else:
            self._instantiate_from_local_path(path, cloud_path, ignore_stored_metadata=hypothetical)

        if hypothetical:
            logger.debug("Ignored stored metadata for %r.", self)
        else:
            if self.metadata(use_octue_namespace=False, include_sdk_version=False) != {
                "id": id or self.id,
                "timestamp": timestamp,
                "tags": TagDict(tags),
                "labels": LabelSet(labels),
            }:
                logger.warning(
                    "Overriding metadata given at instantiation with stored metadata for %r - set `hypothetical` to "
                    "`True` at instantiation to avoid this.",
                    self,
                )

    @classmethod
    def deserialise(cls, serialised_datafile, from_string=False):
        """Deserialise a Datafile from a dictionary or JSON string.

        :param dict|str serialised_datafile:
        :param bool from_string:
        :return Datafile:
        """
        if from_string:
            serialised_datafile = json.loads(serialised_datafile, cls=OctueJSONDecoder)
        else:
            serialised_datafile = copy.deepcopy(serialised_datafile)

        cloud_metadata = serialised_datafile.pop("_cloud_metadata", {})
        datafile = Datafile(**serialised_datafile)
        datafile._cloud_metadata = cloud_metadata
        return datafile

    @property
    def name(self):
        """Get the name of the datafile.

        :return str:
        """
        if self._name:
            return self._name

        if self.exists_in_cloud:
            return str(storage.path.split(self.cloud_path)[-1]).split("?")[0]

        return str(os.path.split(self.local_path)[-1])

    @property
    def extension(self):
        """Get the extension of the datafile.

        :return str:
        """
        return os.path.splitext(self.name)[-1].strip(".")

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
            self.upload(cloud_path=path)

    @property
    def cloud_hash_value(self):
        """Get the hash value of the datafile according to its cloud file.

        :return str|None: `None` if no cloud metadata is available
        """
        return self._cloud_metadata.get("crc32c")

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

        :return float|None:
        """
        if self.exists_in_cloud:
            last_modified = self._cloud_metadata.get("updated")

            if last_modified is None:
                return None

            return last_modified.timestamp()

        try:
            return os.path.getmtime(self.local_path)
        except FileNotFoundError:
            return None

    @property
    def size_bytes(self):
        """Get the size of the datafile in bytes.

        :return float|None:
        """
        if self.exists_in_cloud:
            return self._cloud_metadata.get("size")

        try:
            return os.path.getsize(self.local_path)
        except FileNotFoundError:
            return None

    @property
    def exists_locally(self):
        """Return `True` if the file exists locally.

        :return bool:
        """
        return self._local_path is not None

    @property
    def local_path(self):
        """Get the local path for the datafile, downloading it from the cloud to a temporary file if necessary.

        :return str: The local path of the datafile.
        """
        if self._local_path:
            return self._local_path

        return self.download()

    @property
    def path(self):
        """Alias to the `local_path` property.

        :return str:
        """
        return self.local_path

    @local_path.setter
    def local_path(self, path):
        """Set the local path of the datafile and:
        - If it exists in the cloud, download the contents of the corresponding cloud file to the new local path
        - If it only exists locally, copy the contents of the old local path to the new local path

        :param str path:
        :raise octue.exceptions.CloudLocationNotSpecified: if `path` is `None` and the datafile doesn't exist in the cloud
        :raise FileExistsError: if the new path corresponds to an existing local file
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
                "existing file would overwrite the existing file at the given path."
            )

        if self.exists_in_cloud:
            GoogleCloudStorageClient().download_to_file(local_path=path, cloud_path=self.cloud_path)
        else:
            os.makedirs(os.path.split(path)[0], exist_ok=True)
            shutil.copy(self._local_path, path)

        self._local_path = os.path.abspath(path)

    @property
    def open(self):
        """Open the datafile for reading/writing. Usage is the same as the `python built-in open context manager
        <https://docs.python.org/3/library/functions.html#open>`_ but it can only be used as a context manager e.g.

        .. code-block::

            with datafile.open("w") as f:
                f.write("some data")
        """
        return functools.partial(_DatafileContextManager, self)

    @property
    def _local_metadata_path(self):
        """Get the path to the datafile's local metadata file (if the datafile exists locally).

        :return str|None:
        """
        if not self.exists_locally:
            return None

        return os.path.join(os.path.dirname(self._local_path), METADATA_FILENAME)

    def __enter__(self):
        self._open_context_manager = self.open(**self._open_attributes)
        return self, self._open_context_manager.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._open_context_manager.__exit__(exc_type, exc_val, exc_tb)
        del vars(self)["_open_context_manager"]

    def __lt__(self, other):
        if not isinstance(other, Datafile):
            raise TypeError(f"An object of type {type(self)} cannot be compared with {type(other)}.")
        return self.name < other.name

    def __gt__(self, other):
        if not isinstance(other, Datafile):
            raise TypeError(f"An object of type {type(self)} cannot be compared with {type(other)}.")
        return self.name > other.name

    def upload(self, cloud_path=None, update_cloud_metadata=True):
        """Upload a datafile to Google Cloud Storage.

        :param str|None cloud_path: full path to cloud storage location to store datafile at (e.g. `gs://bucket_name/path/to/file.csv`)
        :param bool update_cloud_metadata: if `True`, update the metadata of the datafile in the cloud at upload time
        :return str: gs:// path for datafile
        """
        cloud_path = self._get_cloud_location(cloud_path)

        self._get_cloud_metadata()

        storage_client = GoogleCloudStorageClient()

        # If the there is no cloud file or if the datafile's file has been changed locally, overwrite its cloud copy.
        if not storage_client.exists(cloud_path) or self.cloud_hash_value != self.hash_value:
            storage_client.upload_file(
                local_path=self.local_path,
                cloud_path=cloud_path,
                metadata=self.metadata(),
            )

        if update_cloud_metadata:
            # If the datafile's metadata has been changed locally, update the cloud file's metadata.
            local_metadata = self.metadata(use_octue_namespace=False)

            if self._cloud_metadata.get("custom_metadata") != local_metadata:
                self.update_cloud_metadata()

        return self.cloud_path

    def download(self, local_path=None):
        """Download the file from the cloud to the given local path or a temporary path if none is given.

        :param str|None local_path: The local path to download the datafile to. A temporary path is used if none is given.
        :raise octue.exceptions.CloudLocationNotSpecified: If the datafile does not exist in the cloud
        :return str: The path to the local file
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

        # Download from a URL.
        if storage.path.is_url(self.cloud_path):
            with open(self._local_path, "wb") as f:
                f.write(requests.get(self.cloud_path).content)

        # Download from a cloud URI.
        else:
            try:
                GoogleCloudStorageClient().download_to_file(local_path=self._local_path, cloud_path=self.cloud_path)

            except google.api_core.exceptions.NotFound as e:
                # If in reading mode, raise an error if no file exists at the path; if in a writing mode, create a new file.
                if self._open_attributes["mode"] == "r":
                    raise e

        # Now use hash value of local file instead of cloud file.
        self.reset_hash()
        return self._local_path

    def metadata(self, include_id=True, include_sdk_version=True, use_octue_namespace=True):
        """Get the datafile's metadata in a serialised form (i.e. the attributes `id`, `timestamp`, `labels`, `tags`,
        and `sdk_version`).

        :param bool include_id: if `True`, include the ID of the datafile
        :param bool include_sdk_version: if `True`, include the `octue` version that instantiated the datafile in the metadata
        :param bool use_octue_namespace: if `True`, prefix metadata names with "octue__"
        :return dict:
        """
        metadata = super().metadata(include_sdk_version=include_sdk_version, include_id=include_id)

        if not use_octue_namespace:
            return metadata

        return {f"{OCTUE_METADATA_NAMESPACE}__{key}": value for key, value in metadata.items()}

    def update_metadata(self):
        """If the datafile is cloud-based, update its cloud metadata; otherwise, update its local metadata.

        :return None:
        """
        if self.exists_in_cloud:
            self.update_cloud_metadata()
            return

        self.update_local_metadata()

    def update_cloud_metadata(self):
        """Update the cloud metadata for the datafile.

        :return None:
        """
        if not self.cloud_path:
            self._raise_cloud_location_error()

        GoogleCloudStorageClient().overwrite_custom_metadata(self.cloud_path, metadata=self.metadata())

    def update_local_metadata(self):
        """Create or update the local octue metadata file with the datafile's metadata.

        :return None:
        """
        existing_metadata_records = load_local_metadata_file(self._local_metadata_path)

        if not existing_metadata_records.get("datafiles"):
            existing_metadata_records["datafiles"] = {}

        existing_metadata_records["datafiles"][self.name] = self.metadata(use_octue_namespace=False)
        overwrite_local_metadata_file(data=existing_metadata_records, path=self._local_metadata_path)

    def generate_signed_url(self, expiration=datetime.timedelta(days=7)):
        """Generate a signed URL for the datafile.

        :param datetime.datetime|datetime.timedelta expiration: the amount of time or date after which the URL should expire
        :return str: the signed URL for the datafile
        """
        if not self.exists_in_cloud:
            raise CloudLocationNotSpecified(
                f"{self!r} must exist in the cloud for a signed URL to be generated for it."
            )

        return GoogleCloudStorageClient().generate_signed_url(cloud_path=self.cloud_path, expiration=expiration)

    def _instantiate_from_cloud_object(self, path, local_path, ignore_stored_metadata):
        """Instantiate the datafile from a cloud object.

        :param str path: the cloud path to a cloud object
        :param str|None local_path: a local path to use for opening or modifying the cloud object locally
        :param bool ignore_stored_metadata: if `True`, don't use any metadata stored for this datafile in the cloud
        :return None:
        """
        self._cloud_path = path

        if not ignore_stored_metadata:
            self._use_cloud_metadata()

        if local_path:
            # If there is no file at the given local path or the file is different to the one in the cloud, download
            # the cloud file locally.
            if not os.path.exists(local_path) or self.cloud_hash_value != calculate_file_hash(local_path):
                self.download(local_path)
            else:
                self._local_path = local_path

    def _instantiate_from_local_path(self, path, cloud_path, ignore_stored_metadata):
        """Instantiate the datafile from a local path.

        :param str path: the path to a local file
        :param str|None cloud_path: a cloud path to upload the datafile to and mirror any changes made to it locally
        :param bool ignore_stored_metadata: if `True`, don't use any metadata stored for this datafile locally
        :return None:
        """
        self._local_path = os.path.abspath(path)

        if not ignore_stored_metadata:
            self._use_local_metadata()

        if cloud_path:
            self.cloud_path = cloud_path

    def _get_cloud_metadata(self):
        """Get the cloud metadata for the datafile and store it without updating the datafile's metadata.

        :return None:
        """
        if not self.cloud_path:
            self._raise_cloud_location_error()

        if storage.path.is_url(self.cloud_path):
            cloud_metadata = {"custom_metadata": {}}
            headers = requests.head(self.cloud_path).headers

            # Get any Octue custom metadata from the datafile.
            for key, value in headers.items():
                if OCTUE_METADATA_NAMESPACE not in key:
                    continue

                # Decode custom metadata values from JSON.
                try:
                    value = json.loads(value, cls=OctueJSONDecoder)
                except json.decoder.JSONDecodeError:
                    pass

                # Remove the "x-goog-meta-" prefix from custom metadata key names.
                cloud_metadata["custom_metadata"][key.replace("x-goog-meta-", "")] = value

            # Store what non-custom metadata is available from the XML headers.
            cloud_metadata["size"] = int(headers.get("Content-Length"))
            cloud_metadata["crc32c"] = headers.get("x-goog-hash", "").split(",")[0].replace("crc32c=", "")

        else:
            cloud_metadata = GoogleCloudStorageClient().get_metadata(cloud_path=self.cloud_path)

        if cloud_metadata:
            cloud_metadata["custom_metadata"] = {
                key.replace(f"{OCTUE_METADATA_NAMESPACE}__", ""): value
                for key, value in cloud_metadata["custom_metadata"].items()
            }

            self._cloud_metadata = cloud_metadata

    def _use_cloud_metadata(self):
        """Update the datafile's metadata from the metadata of the cloud object located at its path. If no metadata is
        stored for the datafile, do nothing.

        :return None:
        """
        self._get_cloud_metadata()
        cloud_custom_metadata = self._cloud_metadata.get("custom_metadata", {})

        if not cloud_custom_metadata:
            return

        self.immutable_hash_value = self.cloud_hash_value or EMPTY_STRING_HASH_VALUE
        self._set_metadata(cloud_custom_metadata)

    def _use_local_metadata(self):
        """Update the datafile's metadata from the local metadata records file. If no metadata is stored for the
        datafile, do nothing.

        :return None:
        """
        existing_metadata_records = load_local_metadata_file(self._local_metadata_path)
        datafile_metadata = existing_metadata_records.get("datafiles", {}).get(self.name, {})

        if not datafile_metadata:
            return

        self._set_metadata(datafile_metadata)

    def _set_metadata(self, metadata):
        """Set the datafile's metadata.

        :param dict metadata:
        :return None:
        """
        for attribute in self._METADATA_ATTRIBUTES:
            if attribute not in metadata:
                continue

            if attribute == "id":
                self._set_id(metadata["id"])
                continue

            setattr(self, attribute, metadata[attribute])

    def _metadata_hash_value(self):
        """Get the hash of the datafile's metadata, not including its ID.

        :return str:
        """
        return super()._metadata_hash_value(use_octue_namespace=False)

    def _calculate_hash(self):
        """Get the hash of the datafile according to the first of the following methods that is applicable:

        1. The hash of the file at its local path
        2. If it doesn't have a local path, the hash of the file at its cloud path
        3. If it doesn't have either of these, use the empty string hash value

        :return str:
        """
        if self.exists_locally and os.path.exists(self._local_path):
            # Calculate the hash of the file itself and then pass it to `Hashable` to include the hashes of any
            # attributes named in `self._ATTRIBUTES_TO_HASH`.
            hash_value = calculate_file_hash(self._local_path)
            return super()._calculate_hash(hash_value)
        else:
            return self.cloud_hash_value or EMPTY_STRING_HASH_VALUE


class _DatafileContextManager:
    """A context manager for opening datafiles for reading and writing locally or from the cloud. Its usage is analogous
    to the builtin `open` context manager. If opening a local datafile in write mode, the manager will attempt to
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
    os.makedirs(os.path.split(my_datafile.local_path)[0], exist_ok=True)
    with open(my_datafile.local_path, 'w') as fp:
        fp.write("{}")
    ```

    :param octue.resources.datafile.Datafile datafile:
    :param str mode: open the datafile for reading/editing in this mode (the mode options are the same as for the built-in ``open`` function)
    :param bool update_metadata: if this is ``True``, update the stored metadata of the datafile when the context is exited
    :return None:
    """

    MODIFICATION_MODES = {"w", "a", "x", "+", "U"}

    def __init__(self, datafile, mode="r", update_metadata=True, **kwargs):
        self.datafile = datafile
        self.mode = mode
        self._update_metadata = update_metadata
        self.kwargs = kwargs
        self._fp = None

        # Update the open mode on the datafile as it's used in `Datafile` methods.
        self.datafile._open_attributes["mode"] = self.mode

    def __enter__(self):
        """Open the datafile, first downloading it from the cloud if necessary.

        :return io.TextIOWrapper:
        """
        if "w" in self.mode:
            if self.datafile.exists_in_cloud and storage.path.is_url(self.datafile.cloud_path):
                raise ReadOnlyResource(f"{self.datafile} is read-only. Change the open mode to 'r' to continue.")

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

            if self.datafile.exists_in_cloud:
                self.datafile.upload(update_cloud_metadata=self._update_metadata)

            elif self._update_metadata:
                self.datafile.update_local_metadata()


def calculate_file_hash(path):
    """Calculate the hash of the file at the given path.

    :param str path:
    :return google_crc32c.Checksum:
    """
    hash = Checksum()

    with open(path, "rb") as f:
        # Read and update hash value in blocks of 4K.
        for byte_block in iter(lambda: f.read(4096), b""):
            hash.update(byte_block)

    return hash
