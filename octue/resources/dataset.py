import logging

from octue.exceptions import InvalidInputException, UnexpectedNumberOfResultsException
from octue.mixins import Identifiable, Loggable, Serialisable, Taggable
from octue.resources.datafile import Datafile


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
            if args[0].__class__.__name__ != "DataFile":
                raise InvalidInputException(
                    'Object "{}" must be of class DataFile to append it to a manifest'.format(args[0])
                )
            self.files.append(args[0])

        else:
            # Append a single file, constructed by passing the arguments through to DataFile()
            self.files.append(Datafile(**kwargs))

    def get_files(self, method="name_icontains", files=None, filter_value=None):
        """ Get a list of data files in a manifest whose name contains the input string

        TODO improved comprehension for compact search syntax here.
         Searching in different fields, dates date ranges, case sensitivity, search in path, metadata searches,
         filestartswith, search indexing of files, etc etc. Could have a list of tuples with different criteria, AND
         them or OR them.

        TODO consider returning a function that acts as a chainable filter so you can do like:
         my_dataset.filter('sequence__not', True).filter('extension', 'csv').filter('posix_timestamp__between', [123456, 135790]).all()
         The all() method should yield a generator. Basically, we're talking about django's filtering here!

        :return: results list of matching datafiles
        """

        # Search through the input list of files or by default all files in the manifest
        files = files if files else self.files

        results = []
        for file in files:
            if method == "name_icontains" and filter_value.lower() in file.name.lower():
                results.append(file)
            if method == "name_contains" and filter_value in file.name:
                results.append(file)
            if method == "name_endswith" and file.name.endswith(filter_value):
                results.append(file)
            if method == "tag_exact" and filter_value in file.tags:
                results.append(file)
            if method == "tag_startswith":
                for tag in file.tags:
                    if tag.startswith(filter_value):
                        results.append(file)
                        break
            if method == "tag_endswith":
                for tag in file.tags:
                    if tag.endswith(filter_value):
                        results.append(file)
                        break
            if method == "in_sequence":
                if file.sequence is not None:
                    results.append(file)

        return results

    def get_file_sequence(self, method="name_icontains", filter_value=None, files=None):
        """ Get an ordered sequence of files matching a criterion

        Accepts the same search arguments as `get_files`.

        """

        results = self.get_files(filter_value=filter_value, method=method, files=files)
        results = self.get_files(method="in_sequence", files=results)

        def get_sequence_number(file):
            return file.sequence

        # Sort the results on ascending sequence number
        results.sort(key=get_sequence_number)

        # TODO check sequence is unique and sequential!!!
        return results

    def get_file_by_tag(self, tag_string):
        """ Gets a data file from a manifest by searching for files with the provided tag(s)

        Gets exclusively one file; if no file or more than one file is found this results in an error.

        :param tag_string: if this string appears as an exact match in the tags
        :return: DataFile object
        """
        results = self.get_files(method="tag_exact", filter_value=tag_string)
        if len(results) > 1:
            raise UnexpectedNumberOfResultsException("More than one result found when searching for a file by tag")
        elif len(results) == 0:
            raise UnexpectedNumberOfResultsException("No files found with this tag")
