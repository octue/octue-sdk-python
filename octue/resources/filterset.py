BASE_TYPES = (set, list)


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
    return instance.__class__((item for item in instance if item.check_attribute(filter_name, filter_value)))


# This defines FilterSet and FilterList classes that inherit from `set` and `list` respectively while implementing a
# `filter` method (i.e. `_filter` above).
for type_ in BASE_TYPES:
    class_name = f"Filter{type_.__name__.title()}"
    globals()[class_name] = type(class_name, (type_,), {"filter": _filter})
