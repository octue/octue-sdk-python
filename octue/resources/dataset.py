import concurrent.futures
import copy
import datetime
import json
import logging
import os
import tempfile

import coolname
import requests

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import CloudLocationNotSpecified, InvalidInputException
from octue.mixins import CloudPathable, Hashable, Identifiable, Labelable, Metadata, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.resources.filter_containers import FilterSet
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.metadata import METADATA_FILENAME, load_local_metadata_file, overwrite_local_metadata_file


logger = logging.getLogger(__name__)


SIGNED_METADATA_DIRECTORY = ".signed_metadata_files"


class Dataset(Labelable, Taggable, Serialisable, Identifiable, Hashable, Metadata, CloudPathable):
    """A representation of a dataset, containing files, labels, etc.

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.

    :param iter(dict|octue.resources.datafile.Datafile) files: the files belonging to the dataset
    :param str|None name:
    :param str|None id:
    :param str|None path:
    :param dict|octue.resources.tag.TagDict|None tags:
    :param iter(str)|octue.resources.label.LabelSet|None labels:
    :param bool recursive: if `True`, include in the dataset all files in the subdirectories recursively contained within the dataset directory
    :param bool hypothetical: if `True`, ignore any metadata stored for this dataset locally or in the cloud and use whatever is given at instantiation
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("files",)
    _METADATA_ATTRIBUTES = ("id", "name", "tags", "labels")

    # Paths to files are added to the serialisation in `Dataset.to_primitive`.
    _SERIALISE_FIELDS = (*_METADATA_ATTRIBUTES, "path")

    def __init__(
        self,
        files=None,
        name=None,
        id=None,
        path=None,
        tags=None,
        labels=None,
        recursive=False,
        hypothetical=False,
    ):
        super().__init__(name=name, id=id, tags=tags, labels=labels)
        self.path = path or os.getcwd()
        self.files = FilterSet()

        if files:
            self.files = self._instantiate_datafiles(files)
            return

        if storage.path.is_cloud_path(self.path):
            self._instantiate_from_cloud(path=self.path, recursive=recursive, hypothetical=hypothetical)
        else:
            self._instantiate_from_local_directory(path=self.path, recursive=recursive, hypothetical=hypothetical)

        if hypothetical:
            logger.debug("Ignored stored metadata for %r.", self)
            return

        if self.metadata(include_sdk_version=False) != {
            "name": name or self.name,
            "id": id or self.id,
            "tags": TagDict(tags),
            "labels": LabelSet(labels),
        }:
            logger.warning(
                "Overriding metadata given at instantiation with stored metadata for %r - set `hypothetical` to `True` "
                "at instantiation to avoid this.",
                self,
            )

    @property
    def name(self):
        """Get the name of the dataset

        :return str:
        """
        if self._name:
            return self._name

        if self.exists_in_cloud:
            return storage.path.split(self.path)[-1].split("?")[0]

        return os.path.split(os.path.abspath(os.path.split(self.path)[-1]))[-1]

    @name.setter
    def name(self, name):
        """Set the name of the dataset.

        :param str name:
        :return None:
        """
        self._name = name

    @property
    def exists_locally(self):
        """Return `True` if the dataset exists locally.

        :return bool:
        """
        return not self.exists_in_cloud

    @property
    def all_files_are_in_cloud(self):
        """Do all the files of the dataset exist in the cloud?

        :return bool:
        """
        return all(file.exists_in_cloud for file in self.files)

    @property
    def _metadata_path(self):
        """Get the path to the dataset's metadata file.

        :return str:
        """
        if self.exists_in_cloud:
            if storage.path.is_url(self.path):
                return self.path

            return storage.path.join(self.path, METADATA_FILENAME)

        return os.path.join(self.path, METADATA_FILENAME)

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    def __contains__(self, item):
        return item in self.files

    def __enter__(self):
        """Enter the dataset metadata updating context.

        :return Dataset:
        """
        return self

    def __exit__(self, *args):
        """Update the cloud or local metadata for the dataset.

        :return None:
        """
        self.update_metadata()

    def to_cloud(self, cloud_path=None):
        """Upload a dataset to the given cloud path.

        :param str|None cloud_path: cloud path to store dataset at (e.g. `gs://bucket_name/path/to/dataset`)
        :return str: cloud path for dataset
        """
        cloud_path = self._get_cloud_location(cloud_path)

        files_and_paths = []

        for datafile in self.files:
            if self.exists_in_cloud:
                path_type = "cloud_path"
            else:
                path_type = "local_path"

            datafile_path_relative_to_dataset = self._datafile_path_relative_to_self(datafile, path_type=path_type)

            files_and_paths.append(
                (
                    datafile,
                    storage.path.join(cloud_path, *datafile_path_relative_to_dataset.split(os.path.sep)),
                )
            )

        def upload(iterable_element):
            """Upload a datafile to the given cloud path.

            :param tuple(octue.resources.datafile.Datafile, str) iterable_element:
            :return str:
            """
            datafile = iterable_element[0]
            cloud_path = iterable_element[1]
            datafile.to_cloud(cloud_path=cloud_path)
            return datafile.cloud_path

        # Use multiple threads to significantly speed up file uploads by reducing latency.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for path in executor.map(upload, files_and_paths):
                logger.debug("Uploaded datafile to %r.", path)

        self.path = cloud_path
        self.update_cloud_metadata()
        return cloud_path

    def update_metadata(self):
        """If the dataset is cloud-based, update its cloud metadata; otherwise, update its local metadata.

        :return None:
        """
        if self.exists_in_cloud:
            self.update_cloud_metadata()
            return

        self.update_local_metadata()

    def update_cloud_metadata(self):
        """Create or update the cloud metadata file for the dataset.

        :return None:
        """
        existing_metadata_records = self._get_cloud_metadata()
        existing_metadata_records["dataset"] = self.to_primitive(include_files=False)

        GoogleCloudStorageClient().upload_from_string(
            string=json.dumps(existing_metadata_records, cls=OctueJSONEncoder),
            cloud_path=self._metadata_path,
        )

    def update_local_metadata(self):
        """Create or update the local octue metadata file with the dataset's metadata.

        :return None:
        """
        existing_metadata_records = load_local_metadata_file(self._metadata_path)
        existing_metadata_records["dataset"] = self.to_primitive(include_files=False)
        os.makedirs(self.path, exist_ok=True)
        overwrite_local_metadata_file(data=existing_metadata_records, path=self._metadata_path)

    def generate_signed_url(self, expiration=datetime.timedelta(days=7)):
        """Generate a signed URL for the dataset. This is done by uploading a uniquely named metadata file containing
        signed URLs to the datasets' files and returning a signed URL to that metadata file.

        :param datetime.datetime|datetime.timedelta expiration: the amount of time or date after which the URL should expire
        :return str: the signed URL for the dataset
        """
        if not self.exists_in_cloud:
            raise CloudLocationNotSpecified(
                f"{self!r} must exist in the cloud for a signed URL to be generated for it."
            )

        signed_metadata = self.to_primitive()
        signed_metadata["files"] = [datafile.generate_signed_url(expiration=expiration) for datafile in self.files]

        path_to_signed_metadata_file = storage.path.join(self.path, SIGNED_METADATA_DIRECTORY, coolname.generate_slug())

        storage_client = GoogleCloudStorageClient()

        storage_client.upload_from_string(
            string=json.dumps(signed_metadata, cls=OctueJSONEncoder),
            cloud_path=path_to_signed_metadata_file,
        )

        return storage_client.generate_signed_url(cloud_path=path_to_signed_metadata_file, expiration=expiration)

    def add(self, datafile, path_in_dataset=None):
        """Add a datafile to the dataset. If the datafile's location is outside the dataset, it is copied to the dataset
        root or to the `path_in_dataset` if provided.

        :param octue.resources.datafile.Datafile datafile: the datafile to add to the dataset
        :param str|None path_in_dataset: if provided, set the datafile's local path to this path within the dataset
        :raise octue.exceptions.InvalidInputException: if the datafile is not a `Datafile` instance
        :return None:
        """
        if not isinstance(datafile, Datafile):
            raise InvalidInputException(f"{datafile!r} must be of type `Datafile` to add it to the dataset.")

        if self.exists_in_cloud:
            new_cloud_path = storage.path.join(self.path, path_in_dataset or datafile.name)

            # Add a cloud datafile to a cloud dataset.
            if datafile.exists_in_cloud:

                if datafile.cloud_path != new_cloud_path and not datafile.cloud_path.startswith(self.path):
                    datafile.to_cloud(new_cloud_path)

                self.files.add(datafile)
                return

            # Add a local datafile to a cloud dataset.
            datafile.to_cloud(new_cloud_path)
            self.files.add(datafile)
            return

        new_local_path = os.path.join(self.path, path_in_dataset or datafile.name)

        # Add a cloud datafile to a local dataset.
        if datafile.exists_in_cloud:
            datafile.download(local_path=new_local_path)
            self.files.add(datafile)
            return

        # Add a local datafile to a local dataset.
        if datafile.local_path != new_local_path and not datafile.local_path.startswith(self.path):
            datafile.local_path = new_local_path

        self.files.add(datafile)

    def get_file_by_label(self, label):
        """Get a single datafile from a dataset by filtering for files with the provided label.

        :param str label: the label to filter for
        :raise octue.exceptions.UnexpectedNumberOfResultsException: if zero or more than one results satisfy the filters
        :return octue.resources.datafile.DataFile:
        """
        return self.files.one(labels__contains=label)

    def download_all_files(self, local_directory=None):
        """Download all files in the dataset into the given local directory. If no path to a local directory is given,
        the files will be downloaded to temporary locations.

        :param str|None local_directory:
        :return None:
        """
        if not self.exists_in_cloud:
            raise CloudLocationNotSpecified(
                f"You can only download files from a cloud dataset. This dataset's path is {self.path!r}."
            )

        local_directory = local_directory or tempfile.TemporaryDirectory().name

        files_and_paths = []

        for file in self.files:

            if not file.exists_in_cloud:
                continue

            path_relative_to_dataset = self._datafile_path_relative_to_self(file, path_type="cloud_path")

            local_path = os.path.abspath(os.path.join(local_directory, *path_relative_to_dataset.split("/")))
            files_and_paths.append((file, local_path))

        def download(iterable_element):
            """Download a datafile to the given path.

            :param tuple(octue.resources.datafile.Datafile, str) iterable_element:
            :return str:
            """
            datafile = iterable_element[0]
            local_path = iterable_element[1]
            return datafile.download(local_path=local_path)

        # Use multiple threads to significantly speed up files downloads by reducing latency.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for path in executor.map(download, files_and_paths):
                logger.debug("Downloaded datafile to %r.", path)

        logger.info("Downloaded %r to %r.", self, local_directory)

    def to_primitive(self, include_files=True):
        """Convert the dataset to a dictionary of primitives, converting its files into their paths for a lightweight
        serialisation.

        :param bool include_files: if `True`, include the `files` parameter in the dictionary
        :return dict:
        """
        serialised_dataset = super().to_primitive()

        if self.exists_in_cloud:
            path_type = "cloud_path"
        else:
            path_type = "local_path"

        if include_files:
            serialised_dataset["files"] = sorted(getattr(datafile, path_type) for datafile in self.files)

        return serialised_dataset

    def _instantiate_from_cloud(self, path, recursive=True, hypothetical=False):
        """Instantiate the dataset from a cloud directory.

        :param str path: the cloud path to a directory in cloud storage
        :param bool recursive: if `True`, include in the dataset all files in the subdirectories recursively contained within the dataset directory
        :param bool hypothetical: if `True`, ignore any metadata stored for this dataset in the cloud and use whatever is given at instantiation
        :return None:
        """
        if not hypothetical:
            self._use_cloud_metadata()

        if not self.files:
            bucket_name = storage.path.split_bucket_name_from_cloud_path(path)[0]

            self.files = FilterSet(
                Datafile(path=storage.path.generate_gs_path(bucket_name, blob.name))
                for blob in GoogleCloudStorageClient().scandir(
                    path,
                    recursive=recursive,
                    filter=(
                        lambda blob: (
                            not blob.name.endswith(METADATA_FILENAME) and SIGNED_METADATA_DIRECTORY not in blob.name
                        )
                    ),
                )
            )

    def _instantiate_from_local_directory(self, path, recursive=False, hypothetical=False):
        """Instantiate the dataset from a local directory.

        :param str path: the path to a local directory
        :param bool recursive: if `True`, include in the dataset all files in the subdirectories recursively contained within the dataset directory
        :param bool hypothetical: if `True`, ignore any metadata stored for this dataset locally and use whatever is given at instantiation
        :return None:
        """
        self.files = FilterSet()

        for level, (directory_path, _, filenames) in enumerate(os.walk(path)):
            for filename in filenames:

                if filename == METADATA_FILENAME:
                    continue

                if not recursive and level > 0:
                    break

                self.files.add(Datafile(path=os.path.join(directory_path, filename)))

        if not hypothetical:
            self._use_local_metadata()

    def _instantiate_datafiles(self, files):
        """Instantiate and add the given files to a `FilterSet`.

        :param iter(str|dict|octue.resources.datafile.Datafile) files:
        :return octue.resources.filter_containers.FilterSet:
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return FilterSet(executor.map(self._instantiate_datafile, copy.deepcopy(files)))

    def _instantiate_datafile(self, file):
        """Instantiate a datafile from multiple input formats.

        :param str|dict|octue.resources.datafile.Datafile file:
        :return octue.resources.datafile.Datafile:
        """
        if isinstance(file, Datafile):
            return file

        if isinstance(file, str):
            return Datafile(path=file)

        return Datafile.deserialise(file)

    def _get_cloud_metadata(self):
        """Get the cloud metadata for the given dataset if a dataset metadata file has previously been uploaded.

        :param str cloud_path: the path to the dataset cloud directory
        :return dict: the dataset metadata or an empty dictionary if there isn't any
        """
        if storage.path.is_url(self.path):
            try:
                return requests.get(self.path).json()
            except requests.exceptions.ConnectionError:
                return {}

        storage_client = GoogleCloudStorageClient()

        if not storage_client.exists(cloud_path=self._metadata_path):
            return {}

        return json.loads(storage_client.download_as_string(cloud_path=self._metadata_path)).get("dataset", {})

    def _use_cloud_metadata(self):
        """Update the dataset instance's metadata from the metadata file located in its cloud directory. If no metadata
        is stored for the dataset, do nothing.

        :return None:
        """
        dataset_metadata = self._get_cloud_metadata()

        if not dataset_metadata:
            return

        self._set_metadata(dataset_metadata)

    def _use_local_metadata(self):
        """Update the dataset instance's metadata from the local metadata records file. If no metadata is stored for the
        dataset, do nothing.

        :return None:
        """
        local_metadata = load_local_metadata_file(self._metadata_path)
        dataset_metadata = local_metadata.get("dataset", {})

        if not dataset_metadata:
            return

        self._set_metadata(dataset_metadata)

    def _set_metadata(self, metadata):
        """Set the dataset's metadata.

        :param dict metadata:
        :return None:
        """
        if "files" in metadata:
            self.files = FilterSet(Datafile(path=path) for path in metadata["files"])

        for attribute in self._METADATA_ATTRIBUTES:
            if attribute not in metadata:
                continue

            if attribute == "id":
                self._set_id(metadata["id"])
                continue

            setattr(self, attribute, metadata[attribute])

    def _datafile_path_relative_to_self(self, datafile, path_type):
        """Get the path of the given datafile relative to the dataset.

        :param octue.resources.datafile.Datafile datafile: the datafile to get the relative path for
        :param str path_type: the datafile path type to use to calculate the relative path - one of "cloud_path" or "local_path"
        :return str: the relative path
        """
        if storage.path.is_url(self.path):
            dataset_path = self.path.split(SIGNED_METADATA_DIRECTORY)[0].strip("/")
        else:
            dataset_path = self.path

        datafile_path = getattr(datafile, path_type)

        if storage.path.is_url(datafile_path):
            datafile_path = datafile_path.split("?")[0]

        return storage.path.relpath(datafile_path, dataset_path)
