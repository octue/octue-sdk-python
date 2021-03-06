import logging
import os
import warnings

from octue.exceptions import BrokenSequenceException, InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Hashable, Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.resources.filter_containers import FilterSet


module_logger = logging.getLogger(__name__)


class Dataset(Taggable, Serialisable, Pathable, Loggable, Identifiable, Hashable):
    """ A representation of a dataset, containing files, tags, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.
    """

    _FILTERSET_ATTRIBUTE = "files"
    _ATTRIBUTES_TO_HASH = "files", "name", "tags"

    def __init__(self, id=None, logger=None, path=None, path_from=None, base_from=None, tags=None, **kwargs):
        """ Construct a Dataset
        """
        super().__init__(id=id, logger=logger, tags=tags, path=path, path_from=path_from, base_from=base_from)

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then remove this hackjob.
        files = kwargs.pop("files", list())
        self.files = FilterSet()
        for fi in files:
            if isinstance(fi, Datafile):
                self.files.add(fi)
            else:
                self.files.add(Datafile(**fi, path_from=self, base_from=self))

        self.__dict__.update(**kwargs)

    @property
    def name(self):
        return str(os.path.split(self.path)[-1])

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    def add(self, *args, **kwargs):
        """ Add a data/results file to the manifest

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
        """ Get an ordered sequence of files matching a criterion

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
        """ Gets a data file from a manifest by searching for files with the provided tag(s)

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
