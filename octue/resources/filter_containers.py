from octue import exceptions


def _filter(instance, filter_name=None, filter_value=None):
    """Returns a new instance containing only the Filterables to which the given filter criteria apply.

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


def _order_by(instance, attribute_name, reverse=False):
    """Order the instance by the given attribute_name, returning the instance's elements as a new FilterList (not a
    FilterSet.
    """
    try:
        return FilterList(sorted(instance, key=lambda item: getattr(item, attribute_name), reverse=reverse))
    except AttributeError:
        raise exceptions.InvalidInputException(
            f"An attribute named {attribute_name!r} does not exist on one or more members of {instance!r}."
        )


class FilterSet(set):
    filter = _filter
    order_by = _order_by


class FilterList(list):
    filter = _filter
    order_by = _order_by
