class FilterSet:
    def __init__(self, iterable=None):
        self._set = set(iterable or [])

    def __iter__(self):
        yield from self._set

    def __len__(self):
        return len(self._set)

    def __contains__(self, item):
        return item in self._set

    def __eq__(self, other):
        return self._set == other._set

    def __repr__(self):
        return f"<{type(self).__name__}({self._set})>"

    def add(self, item):
        self._set.add(item)

    def filter(self, filter_name=None, filter_value=None):
        """ Returns a new FilterSet containing only the Filterees to which the given filter criteria apply.

        Say we want to filter by files whose extension equals "csv". We want to be able to do...

        ds = DataSet(... initialise with csv files and other files)
        Example of chaining:

        ds.filter('files__extension_equals', 'csv')
        is equvalent to
        ds.files.filter(extension__equals', 'csv')
        >>> FilterSet containing a set of datafiles


        ds.filter('files__extension_equals', 'csv')
        is equvalent to
        ds.files.filter(extension__equals', 'csv')
        """
        return FilterSet((item for item in self._set if item.check_attribute(filter_name, filter_value)))
