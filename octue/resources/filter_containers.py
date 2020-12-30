def _filter(instance, filter_name=None, filter_value=None):
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
    return instance.__class__((item for item in instance if item.satisfies(filter_name, filter_value)))


class FilterSet(set):
    filter = _filter


class FilterList(list):
    filter = _filter
