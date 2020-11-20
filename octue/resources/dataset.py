import logging

from octue.exceptions import BrokenSequenceException, InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.resources.datafile import Datafile


module_logger = logging.getLogger(__name__)


class Dataset(Taggable, Serialisable, Pathable, Loggable, Identifiable):
    """ A representation of a dataset, containing files, tags, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.
    """

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

        self.__dict__.update(**kwargs)

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

    def get_files(self, field_lookup, files=None, filter_value=None):
        """ Get a list of data files in a manifest whose name contains the input string

        Django field lookups: https://docs.djangoproject.com/en/3.1/ref/models/querysets/

        :parameter field_lookup: filter specifier. Inspired by django queryset filters which in turn mimic SQL syntax.
        Comprised of "<field_name>__<filter_kind>", so 'name__icontains' would perform case-insensitive matching on
        Datafile.name. See https://github.com/octue/octue-sdk-python/issues/7 Currently supported combinations:

            "name__icontains"
            "name__contains"
            "name__endswith"
            "tag__exact"
            "tag__startswith"
            "tag__endswith"
            "sequence__gt"

        :parameter files: List of files to filter. This allows multiple separate filters to be applied. By default the
        entire list of files in the dataset is used.
        :type files: list

        :parameter filter_value: Value, typically of the same type as the field, used to filter against.
        : type filter_value: number, str

        :return: results list of matching datafiles
        :rtype: list(Datafile)
        """

        # Search through the input list of files or by default all files in the dataset
        files = files or self.files

        # Frequent error of typing only a single underscore causes no results to be returned... catch it
        if "__" not in field_lookup:
            raise InvalidInputException(
                f"Invalid field lookup '{field_lookup}'. Field lookups should be in the form '<field_name>__'<filter_kind>"
            )

        field_lookups = {
            "name__icontains": lambda filter_value, file: filter_value.lower() in file.name.lower(),
            "name__contains": lambda filter_value, file: filter_value in file.name,
            "name__endswith": lambda filter_value, file: file.name.endswith(filter_value),
            "name__startswith": lambda filter_value, file: file.name.startswith(filter_value),
            "tag__exact": lambda filter_value, file: filter_value in file.tags,
            "tag__startswith": lambda filter_value, file: file.tags.startswith(filter_value),
            "tag__endswith": lambda filter_value, file: file.tags.endswith(filter_value),
            "tag__contains": lambda filter_value, file: file.tags.contains(filter_value),
            "sequence__notnone": lambda filter_value, file: file.sequence is not None,
        }

        results = []

        for file in files:
            if field_lookups[field_lookup](filter_value, file):
                results.append(file)

        return results

    def get_file_sequence(self, field_lookup, files=None, filter_value=None, strict=True):
        """ Get an ordered sequence of files matching a criterion

        Accepts the same search arguments as `get_files`.

        :parameter strict: If True, applies a check that the resulting file sequence begins at 0 and ascends uniformly
        by 1
        :type strict: bool

        :returns: Sorted list of Datafiles
        :rtype: list(Datafile)
        """

        results = self.get_files(field_lookup, files=files, filter_value=filter_value)
        results = self.get_files("sequence__notnone", files=results)

        def get_sequence_number(file):
            return file.sequence

        # Sort the results on ascending sequence number
        results.sort(key=get_sequence_number)

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
        results = self.get_files("tag__exact", filter_value=tag_string)
        if len(results) > 1:
            raise UnexpectedNumberOfResultsException("More than one result found when searching for a file by tag")
        elif len(results) == 0:
            raise UnexpectedNumberOfResultsException("No files found with this tag")

        return results[0]
