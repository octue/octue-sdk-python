class FilterSet(set):
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
        return FilterSet((item for item in self if item.check_attribute(filter_name, filter_value)))
