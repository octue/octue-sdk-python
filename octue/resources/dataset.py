import json
import logging
import os
import warnings

from octue import definitions
from octue.exceptions import BrokenSequenceException, InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Hashable, Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.resources.filter_containers import FilterSet
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder


module_logger = logging.getLogger(__name__)


DATAFILES_DIRECTORY = "datafiles"


class Dataset(Taggable, Serialisable, Pathable, Loggable, Identifiable, Hashable):
    """A representation of a dataset, containing files, tags, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.
    """

    _FILTERSET_ATTRIBUTE = "files"
    _ATTRIBUTES_TO_HASH = "files", "name", "tags"

    def __init__(self, name=None, id=None, logger=None, path=None, path_from=None, tags=None, **kwargs):
        """Construct a Dataset"""
        super().__init__(id=id, logger=logger, tags=tags, path=path, path_from=path_from)

        self._name = name

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then remove this hackjob.
        self.files = FilterSet()

        for file in kwargs.pop("files", list()):
            if isinstance(file, Datafile):
                self.files.add(file)
            else:
                self.files.add(Datafile(**file, path_from=self))

        self.__dict__.update(**kwargs)

    @classmethod
    def from_cloud(cls, project_name, bucket_name, path_to_dataset_directory):
        """Instantiate a Dataset from Google Cloud storage.

        :param str project_name:
        :param str bucket_name:
        :param str path_to_dataset_directory:
        :return Dataset:
        """
        storage_client = GoogleCloudStorageClient(project_name=project_name)

        serialised_dataset = json.loads(
            storage_client.download_as_string(
                bucket_name=bucket_name,
                path_in_bucket=storage.path.join(path_to_dataset_directory, definitions.DATASET_FILENAME),
            )
        )

        datafiles = FilterSet()

        for blob in storage_client.scandir(
            bucket_name=bucket_name,
            directory_path=path_to_dataset_directory,
            filter=lambda blob: blob.name.split("/")[-1] != definitions.DATASET_FILENAME,
        ):
            datafiles.add(
                Datafile.from_cloud(project_name=project_name, bucket_name=bucket_name, path_in_bucket=blob.name)
            )

        return Dataset(
            id=serialised_dataset["id"],
            path=storage.path.generate_gs_path(bucket_name, path_to_dataset_directory),
            tags=json.loads(serialised_dataset["tags"]),
            files=datafiles,
        )

    @property
    def name(self):
        return self._name or os.path.split(os.path.abspath(os.path.split(self.path)[-1]))[-1]

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    def add(self, *args, **kwargs):
        """Add a data/results file to the manifest

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

    def append(self, *args, **kwargs):
        warnings.warn(
            "The `Dataset.append` method has been deprecated and replaced with `Dataset.add` to reflect that Datafiles "
            "are stored in a set and not a list. Calls to `Dataset.append` will be redirected to the new method for "
            "now, but please use `Datafile.add` in future.",
            DeprecationWarning,
        )
        self.files.add(*args, **kwargs)

    def get_files(self, field_lookup, filter_value=None):
        warnings.warn(
            "The `Dataset.get_files` method has been deprecated and replaced with `Dataset.files.filter`, which has "
            "the same interface but with the `field_lookup` argument renamed to `filter_name`. Calls to "
            "`Dataset.get_files` will be redirected to the new method for now, but please use `Datafile.files.filter` "
            "in future.",
            DeprecationWarning,
        )
        return self.files.filter(filter_name=field_lookup, filter_value=filter_value)

    def get_file_sequence(self, filter_name, filter_value=None, strict=True):
        """Get an ordered sequence of files matching a criterion

        Accepts the same search arguments as `get_files`.

        :parameter strict: If True, applies a check that the resulting file sequence begins at 0 and ascends uniformly
        by 1
        :type strict: bool

        :returns: Sorted list of Datafiles
        :rtype: list(Datafile)
        """

        results = self.files.filter(filter_name=filter_name, filter_value=filter_value)
        results = results.filter("sequence__is_not", None)

        def get_sequence_number(file):
            return file.sequence

        # Sort the results on ascending sequence number
        results = sorted(results, key=get_sequence_number)

        # Check sequence is unique and sequential
        if strict:
            index = -1
            for result in results:
                index += 1
                if result.sequence != index:
                    raise BrokenSequenceException("Filtered file sequence numbers do not monotonically increase from 0")

        return results

    def get_file_by_tag(self, tag_string):
        """Gets a data file from a manifest by searching for files with the provided tag(s)

        Gets exclusively one file; if no file or more than one file is found this results in an error.

        :param tag_string: if this string appears as an exact match in the tags
        :return: DataFile object
        """
        results = self.files.filter(filter_name="tags__contains", filter_value=tag_string)
        if len(results) > 1:
            raise UnexpectedNumberOfResultsException("More than one result found when searching for a file by tag")
        elif len(results) == 0:
            raise UnexpectedNumberOfResultsException("No files found with this tag")

        return results.pop()

    def serialise(self, shallow=False, to_string=False):
        """Serialise to a dictionary of primitives or to a string. If `shallow` is `True`, the serialised `files`
        field only contains the absolute path of the dataset's files, rather than their entire representation.

        :param bool shallow:
        :param bool to_string:
        :return dict|str:
        """
        if not shallow:
            return super().serialise(to_string=to_string)

        serialised_dataset = super().serialise()
        serialised_dataset["files"] = [file.absolute_path for file in self.files]

        if to_string:
            return json.dumps(serialised_dataset, cls=OctueJSONEncoder, sort_keys=True, indent=4)
        return serialised_dataset

    def to_cloud(self, project_name, bucket_name, output_directory):
        """Upload a dataset to a cloud location.

        :param str project_name:
        :param str bucket_name:
        :param str output_directory:
        :return None:
        """
        for datafile in self.files:
            datafile.to_cloud(
                project_name=project_name,
                bucket_name=bucket_name,
                path_in_bucket=storage.path.join(output_directory, self.name, datafile.name),
            )

        GoogleCloudStorageClient(project_name=project_name).upload_from_string(
            serialised_data=self.serialise(shallow=True, to_string=True),
            bucket_name=bucket_name,
            path_in_bucket=storage.path.join(output_directory, self.name, definitions.DATASET_FILENAME),
        )
