from collections import UserDict

from octue import exceptions


def _filter(self, **kwargs):
    """Returns a new instance containing only the Filterables to which the given filter criteria apply.

    :param {str: any} kwargs: a single keyword argument whose key is the name of the filter and whos value is the value
        to filter for
    :return octue.resources.filter_containers.FilterSet:
    """
    return self.__class__((item for item in self if item.satisfies(**kwargs)))


def _order_by(self, attribute_name, reverse=False):
    """Order the instance by the given attribute_name, returning the instance's elements as a new FilterList (not a
    FilterSet.
    """
    try:
        return FilterList(sorted(self, key=lambda item: getattr(item, attribute_name), reverse=reverse))
    except AttributeError:
        raise exceptions.InvalidInputException(
            f"An attribute named {attribute_name!r} does not exist on one or more members of {self!r}."
        )


class FilterSet(set):
    filter = _filter
    order_by = _order_by


class FilterList(list):
    filter = _filter
    order_by = _order_by


class FilterDict(UserDict):
    order_by = _order_by

    def filter(self, **kwargs):
        return self.__class__({key: value for key, value in self.items() if value.satisfies(**kwargs)})
