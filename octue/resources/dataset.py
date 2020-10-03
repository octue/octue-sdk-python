import logging
import os

from octue.exceptions import BrokenSequenceException, InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Identifiable, Loggable, Serialisable, Taggable
from octue.resources.datafile import Datafile
from octue.utils import isfolder


module_logger = logging.getLogger(__name__)


class Dataset(Taggable, Serialisable, Loggable, Identifiable):
    """ A representation of a dataset, containing files, tags, etc

    This is used to read a list of files (and their associated properties) into octue analysis, or to compile a
    list of output files (results) and their properties that will be sent back to the octue system.
    """

    def __init__(self, id=None, logger=None, tags=None, **kwargs):
        """ Construct a Dataset
        """
        super().__init__(id=id, logger=logger, tags=tags)
        self.files = kwargs.pop("files", list())
        self.__dict__.update(**kwargs)

        # TODO A much better way than relying on the current directory!
        self._path = os.path.abspath(f"./dataset-{self.id}")

    def append(self, *args, **kwargs):
        """ Add a data/results file to the manifest

        Usage:
            my_file = octue.DataFile(...)
            my_manifest.append(my_file)

            # or more simply
            my_manifest.append(**{...}) which implicitly creates the datafile from the starred list of input arguments

        TODO allow for appending a list of datafiles
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
            raise InvalidInputException("Field lookups should be in the form '<field_name>__'<filter_kind>")

        results = []
        for file in files:
            if field_lookup == "name__icontains" and filter_value.lower() in file.name.lower():
                results.append(file)
            if field_lookup == "name__contains" and filter_value in file.name:
                results.append(file)
            if field_lookup == "name__endswith" and file.name.endswith(filter_value):
                results.append(file)
            if field_lookup == "name__startswith" and file.name.startswith(filter_value):
                results.append(file)
            if field_lookup == "tag__exact" and filter_value in file.tags:
                results.append(file)
            if field_lookup == "tag__startswith" and file.tags.startswith(filter_value):
                results.append(file)
            if field_lookup == "tag__endswith" and file.tags.endswith(filter_value):
                results.append(file)
            if field_lookup == "tag__contains" and file.tags.contains(filter_value):
                results.append(file)
            if field_lookup == "sequence__notnone" and file.sequence is not None:
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

    @property
    def path(self):
        # Lazily make the folder if absent
        isfolder(self._path, make_if_absent=True)
        return self._path
