import concurrent.futures
import json
import os

from octue import definitions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.migrations.cloud_storage import translate_bucket_name_and_path_in_bucket_to_cloud_path
from octue.mixins import Hashable, Identifiable, Labelable, Pathable, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.resources.filter_containers import FilterSet
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict


DATAFILES_DIRECTORY = "datafiles"


class Dataset(Labelable, Taggable, Serialisable, Pathable, Identifiable, Hashable):
    """A representation of a dataset, containing files, labels, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.

    :param iter(dict|octue.resources.datafile.Datafile) files: the files belonging to the dataset
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("files",)
    _SERIALISE_FIELDS = "files", "name", "labels", "tags", "id", "path"

    def __init__(self, files=None, name=None, id=None, path=None, path_from=None, tags=None, labels=None, **kwargs):
        super().__init__(name=name, id=id, tags=tags, labels=labels, path=path, path_from=path_from)

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then remove this hackjob.
        self.files = FilterSet()
        self._cloud_path = None

        for file in files or []:
            if isinstance(file, Datafile):
                self.files.add(file)
            else:
                self.files.add(Datafile.deserialise(file, path_from=self))

        self.__dict__.update(**kwargs)

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    @classmethod
    def from_local_directory(cls, path_to_directory, recursive=False, **kwargs):
        """Instantiate a Dataset from the files in the given local directory.

        :param str path_to_directory: path to a local directory
        :param bool recursive: if `True`, include all files in the directory's subdirectories recursively
        :param kwargs: other keyword arguments for the `Dataset` instantiation
        :return Dataset:
        """
        datafiles = FilterSet()

        for level, (directory_path, _, filenames) in enumerate(os.walk(path_to_directory)):
            for filename in filenames:

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

        dataset_metadata = cls._get_dataset_metadata(cloud_path=cloud_path)

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
            for blob in GoogleCloudStorageClient().scandir(cloud_path, recursive=recursive)
        )

        dataset = Dataset(path=cloud_path, files=datafiles)
        dataset._upload_dataset_metadata(cloud_path)
        return dataset

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

        self._upload_dataset_metadata(cloud_path)
        self._cloud_path = cloud_path
        return cloud_path

    @property
    def name(self):
        """Get the name of the dataset

        :return str:
        """
        return self._name or os.path.split(os.path.abspath(os.path.split(self.path)[-1]))[-1]

    @property
    def cloud_path(self):
        """Get the cloud path of the dataset.

        :return str|None:
        """
        return self._cloud_path

    @cloud_path.setter
    def cloud_path(self, path):
        """Set the cloud path of the dataset.

        :param str|None path:
        :return None:
        """
        if path is None:
            self._cloud_path = None
            return

        self.to_cloud(cloud_path=path)

    @property
    def all_files_are_in_cloud(self):
        """Do all the files of the dataset exist in the cloud?

        :return bool:
        """
        return all(file.exists_in_cloud for file in self.files)

    def add(self, *args, **kwargs):
        """Add a data/results file to the manifest.

        Usage:
            my_file = octue.DataFile(...)
            my_manifest.add(my_file)

            # or more simply
            my_manifest.add(**{...}) which implicitly creates the datafile from the starred list of input arguments
        """
        if len(args) > 1:
            # Recurse to allow addition of many files at once
            for arg in args:
                self.add(arg, **kwargs)
        elif len(args) > 0:
            if not isinstance(args[0], Datafile):
                raise InvalidInputException(
                    'Object "{}" must be of class Datafile to add it to a Dataset'.format(args[0])
                )
            self.files.add(args[0])

        else:
            # Add a single file, constructed by passing the arguments through to DataFile()
            self.files.add(Datafile(**kwargs))

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
        files_and_paths = []

        for datafile in self.files:
            if local_directory:
                path_relative_to_dataset = storage.path.relpath(datafile.cloud_path, self.path)
                local_path = os.path.abspath(os.path.join(local_directory, *path_relative_to_dataset.split("/")))
            else:
                local_path = None

            files_and_paths.append((datafile, local_path))

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

    @staticmethod
    def _get_dataset_metadata(cloud_path):
        """Get the metadata for the given dataset if a dataset metadata file has previously been uploaded.

        :param str cloud_path: the path to the dataset cloud directory
        :return dict: the dataset metadata
        """
        storage_client = GoogleCloudStorageClient()
        metadata_file_path = storage.path.join(cloud_path, definitions.DATASET_METADATA_FILENAME)

        if not storage_client.exists(cloud_path=metadata_file_path):
            return {}

        return json.loads(storage_client.download_as_string(cloud_path=metadata_file_path))

    def _upload_dataset_metadata(self, cloud_path):
        """Upload a metadata file representing the dataset to the given cloud location.

        :param str cloud_path: the path to the dataset cloud directory
        :return None:
        """
        serialised_dataset = self.to_primitive()
        serialised_dataset["files"] = sorted(datafile.cloud_path for datafile in self.files)
        del serialised_dataset["path"]

        GoogleCloudStorageClient().upload_from_string(
            string=json.dumps(serialised_dataset),
            cloud_path=storage.path.join(cloud_path, definitions.DATASET_METADATA_FILENAME),
        )
