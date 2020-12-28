import logging
import os

from octue.exceptions import BrokenSequenceException, InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Filterable, Hashable, Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.resources.datafile import Datafile


module_logger = logging.getLogger(__name__)


class Dataset(Taggable, Serialisable, Pathable, Loggable, Identifiable, Hashable, Filterable):
    """ A representation of a dataset, containing files, tags, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.
    """

    _ATTRIBUTES_TO_FILTER_BY = ("files",)
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
        self.files = []
        for fi in files:
            if isinstance(fi, Datafile):
                self.files.append(fi)
            else:
                self.files.append(Datafile(**fi, path_from=self, base_from=self))

        Filterable.__init__(self, filters=self._build_filters())
        self.__dict__.update(**kwargs)

    @property
    def name(self):
        return str(os.path.split(self.path)[-1])

    def __iter__(self):
        yield from self.files

    def __len__(self):
        return len(self.files)

    def _build_filters(self):
        return {
            "name__icontains": ("files", lambda file, filter_value: filter_value.lower() in file.name.lower()),
            "name__contains": ("files", lambda file, filter_value: filter_value in file.name),
            "name__ends_with": ("files", lambda file, filter_value: file.name.endswith(filter_value)),
            "name__starts_with": ("files", lambda file, filter_value: file.name.startswith(filter_value)),
            "tag__exact": ("files", lambda file, filter_value: filter_value in file.tags),
            "tag__starts_with": ("files", lambda file, filter_value: file.tags.starts_with(filter_value)),
            "tag__ends_with": ("files", lambda file, filter_value: file.tags.ends_with(filter_value)),
            "tag__contains": ("files", lambda file, filter_value: file.tags.contains(filter_value)),
            "sequence__notnone": ("files", lambda file, filter_value: file.sequence is not None),
        }

    def append(self, *args, **kwargs):
        """ Add a data/results file to the manifest

        Usage:
            my_file = octue.DataFile(...)
            my_manifest.append(my_file)

            # or more simply
            my_manifest.append(**{...}) which implicitly creates the datafile from the starred list of input arguments

        """
        if len(args) > 1:
            # Recurse to allow addition of many files at once
            for arg in args:
                self.append(arg, **kwargs)
        elif len(args) > 0:
            if not isinstance(args[0], Datafile):
                raise InvalidInputException(
                    'Object "{}" must be of class Datafile to append it to a Dataset'.format(args[0])
                )
            self.files.append(args[0])

        else:
            # Append a single file, constructed by passing the arguments through to DataFile()
            self.files.append(Datafile(**kwargs))

    def get_file_sequence(self, field_lookup, filter_value=None, strict=True):
        """ Get an ordered sequence of files matching a criterion

        Accepts the same search arguments as `get_files`.

        :parameter strict: If True, applies a check that the resulting file sequence begins at 0 and ascends uniformly
        by 1
        :type strict: bool

        :returns: Sorted list of Datafiles
        :rtype: list(Datafile)
        """

        results = self.filter(field_lookup, filter_value=filter_value)
        results = results.filter("sequence__notnone", filter_value=None)

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
        results = self.filter("tag__exact", filter_value=tag_string)
        if len(results) > 1:
            raise UnexpectedNumberOfResultsException("More than one result found when searching for a file by tag")
        elif len(results) == 0:
            raise UnexpectedNumberOfResultsException("No files found with this tag")

        return Dataset(files=[results.files[0]])
