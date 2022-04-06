import concurrent.futures
import json
import logging
import os
import tempfile

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import CloudLocationNotSpecified, InvalidInputException
from octue.migrations.cloud_storage import translate_bucket_name_and_path_in_bucket_to_cloud_path
from octue.mixins import Hashable, Identifiable, Labelable, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.resources.filter_containers import FilterSet
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.local_metadata import LOCAL_METADATA_FILENAME, load_local_metadata_file


logger = logging.getLogger(__name__)


class Dataset(Labelable, Taggable, Serialisable, Identifiable, Hashable):
    """A representation of a dataset, containing files, labels, etc.

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.

    :param iter(dict|octue.resources.datafile.Datafile) files: the files belonging to the dataset
    :param str|None name:
    :param str|None id:
    :param str|None path:
    :param dict|None tags:
    :param iter|None labels:
    :param bool save_metadata_locally: if `True` and the dataset is local, save its metadata to disk locally
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("files",)

    # Paths to files are added to the serialisation in `Dataset.to_primitive`.
    _SERIALISE_FIELDS = "name", "labels", "tags", "id", "path"

    def __init__(self, files=None, name=None, id=None, path=None, tags=None, labels=None, save_metadata_locally=True):
        super().__init__(name=name, id=id, tags=tags, labels=labels)
        self.path = path
        self.files = self._instantiate_datafiles(files or [])

        # Save metadata locally if the dataset exists locally.
        if save_metadata_locally:
            if path and self.exists_locally:
                self._save_local_metadata()

    @classmethod
    def from_local_directory(cls, path_to_directory, recursive=False, **kwargs):
        """Instantiate a Dataset from the files in the given local directory. If a dataset metadata file is present,
        that is used to decide which files are in the dataset.

        :param str path_to_directory: path to a local directory
        :param bool recursive: if `True`, include all files in the directory's subdirectories recursively
        :param kwargs: other keyword arguments for the `Dataset` instantiation
        :return Dataset:
        """
        local_metadata = load_local_metadata_file(os.path.join(path_to_directory, LOCAL_METADATA_FILENAME))
        dataset_metadata = local_metadata.get("dataset")

        if dataset_metadata:
            return Dataset.deserialise(dataset_metadata)

        datafiles = FilterSet()

        for level, (directory_path, _, filenames) in enumerate(os.walk(path_to_directory)):
            for filename in filenames:

                if filename == LOCAL_METADATA_FILENAME:
                    continue

                if not recursive and level > 0:
                    break

                datafiles.add(Datafile(path=os.path.join(directory_path, filename)))

        return Dataset(path=path_to_directory, files=datafiles, **kwargs)

    @classmethod
    def from_cloud(cls, cloud_path=None, bucket_name=None, path_to_dataset_directory=None, recursive=False):
        """Instantiate a Dataset from Google Cloud storage.

        :param str|None cloud_path: full path to dataset directory in cloud storage (e.g. `gs://bucket_name/path/to/dataset`)
        :param bool recursive: if `True`, include in the dataset all files in the subdirectories recursively contained in the dataset directory
        :return Dataset:
        """
        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_to_dataset_directory)

        bucket_name = storage.path.split_bucket_name_from_gs_path(cloud_path)[0]

        dataset_metadata = cls._get_cloud_metadata(cloud_path=cloud_path)

        if dataset_metadata:
            return Dataset(
                id=dataset_metadata.get("id"),
                name=dataset_metadata.get("name"),
                path=cloud_path,
                tags=TagDict(dataset_metadata.get("tags", {})),
                labels=LabelSet(dataset_metadata.get("labels", [])),
                files=[Datafile(path=path) for path in dataset_metadata["files"]],
            )

        datafiles = FilterSet(
            Datafile(path=storage.path.generate_gs_path(bucket_name, blob.name))
            for blob in GoogleCloudStorageClient().scandir(
                cloud_path,
                recursive=recursive,
                filter=lambda blob: not blob.name.endswith(LOCAL_METADATA_FILENAME),
            )
        )

        dataset = Dataset(path=cloud_path, files=datafiles)
        dataset._upload_cloud_metadata()
        return dataset

    @property
    def name(self):
        """Get the name of the dataset

        :return str:
        """
        if self._name:
            return self._name

        if self.exists_in_cloud:
            return storage.path.split(self.path)[-1]

        return os.path.split(os.path.abspath(os.path.split(self.path)[-1]))[-1]

    @property
    def exists_in_cloud(self):
        """Return `True` if the dataset exists in the cloud.

        :return bool:
        """
        return storage.path.is_qualified_cloud_path(self.path)

    @property
    def exists_locally(self):
        """Return `True` if the dataset exists locally.

        :return bool:
        """
        return not storage.path.is_qualified_cloud_path(self.path)

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
            return storage.path.join(self.path, LOCAL_METADATA_FILENAME)

        return os.path.join(self.path, LOCAL_METADATA_FILENAME)

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    def to_cloud(self, cloud_path=None, bucket_name=None, output_directory=None):
        """Upload a dataset to the given cloud path.

        :param str|None cloud_path: cloud path to store dataset at (e.g. `gs://bucket_name/path/to/dataset`)
        :return str: cloud path for dataset
        """
        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, output_directory)

        files_and_paths = []

        for datafile in self.files:
            datafile_path_relative_to_dataset = datafile.path.split(self.path)[-1].strip(os.path.sep).strip("/")
            files_and_paths.append(
                (
                    datafile,
                    storage.path.join(cloud_path, *datafile_path_relative_to_dataset.split(os.path.sep)),
                )
            )

        def upload(iterable_element):
            """Upload a datafile to the given cloud path.

            :param tuple(octue.resources.datafile.Datafile, str) iterable_element:
            :return None:
            """
            datafile = iterable_element[0]
            cloud_path = iterable_element[1]
            datafile.to_cloud(cloud_path=cloud_path)

        # Use multiple threads to significantly speed up file uploads by reducing latency.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(upload, files_and_paths)

        self.path = cloud_path
        self._upload_cloud_metadata()
        return cloud_path

    def add(self, datafile, path_in_dataset=None):
        """Add a datafile to the dataset.

        :param octue.resources.datafile.Datafile datafile: the datafile to add to the dataset
        :param str|None path_in_dataset: if provided, set the datafile's local path to this path within the dataset
        :raise octue.exceptions.InvalidInputException: if the datafile is not a `Datafile` instance
        :return None:
        """
        if not isinstance(datafile, Datafile):
            raise InvalidInputException(f"{datafile!r} must be of type `Datafile` to add it to the dataset.")

        self.files.add(datafile)

        if path_in_dataset:
            datafile.local_path = os.path.join(self.path, path_in_dataset)

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

            path_relative_to_dataset = storage.path.relpath(file.cloud_path, self.path)
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
            executor.map(download, files_and_paths)

        logger.info("Downloaded %r dataset to %r.", self.name, local_directory)

    def to_primitive(self):
        """Convert the dataset to a dictionary of primitives, converting its files into their paths for a lightweight
        serialisation.

        :return dict:
        """
        serialised_dataset = super().to_primitive()

        if self.exists_in_cloud:
            path_type = "cloud_path"
        else:
            path_type = "path"

        serialised_dataset["files"] = sorted(getattr(datafile, path_type) for datafile in self.files)
        return serialised_dataset

    def _instantiate_datafiles(self, files):
        """Instantiate and add the given files to a `FilterSet`.

        :param iter(str|dict|octue.resources.datafile.Datafile) files:
        :return octue.resources.filter_containers.FilterSet:
        """
        files_to_add = FilterSet()

        for file in files:
            if isinstance(file, Datafile):
                files_to_add.add(file)
            elif isinstance(file, str):
                files_to_add.add(Datafile(path=file))
            else:
                files_to_add.add(Datafile.deserialise(file))

        return files_to_add

    @staticmethod
    def _get_cloud_metadata(cloud_path):
        """Get the cloud metadata for the given dataset if a dataset metadata file has previously been uploaded.

        :param str cloud_path: the path to the dataset cloud directory
        :return dict: the dataset metadata
        """
        storage_client = GoogleCloudStorageClient()
        metadata_file_path = storage.path.join(cloud_path, LOCAL_METADATA_FILENAME)

        if not storage_client.exists(cloud_path=metadata_file_path):
            return {}

        return json.loads(storage_client.download_as_string(cloud_path=metadata_file_path))

    def _upload_cloud_metadata(self):
        """Upload a metadata file representing the dataset to the given cloud location.

        :return None:
        """
        GoogleCloudStorageClient().upload_from_string(string=self.serialise(), cloud_path=self._metadata_path)

    def _save_local_metadata(self):
        """Save the dataset metadata locally in the dataset directory.

        :return None:
        """
        os.makedirs(self.path, exist_ok=True)

        existing_metadata_records = load_local_metadata_file(self._metadata_path)
        existing_metadata_records["dataset"] = self.to_primitive()

        with open(self._metadata_path, "w") as f:
            json.dump(existing_metadata_records, f, cls=OctueJSONEncoder)
