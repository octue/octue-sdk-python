from abc import ABC
from collections import UserDict

from octue import exceptions
from octue.mixins import Filterable
from octue.utils.objects import get_nested_attribute


class FilterContainer(ABC):
    def filter(self, ignore_items_without_attribute=True, **kwargs):
        """Return a new instance of the container containing only the `Filterable`s to which the given filter criteria
        are `True`.

        :param bool ignore_items_without_attribute:
        :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the
            values to filter for
        :return octue.resources.filter_containers.FilterContainer:
        """
        pass

    def order_by(self, attribute_name, reverse=False):
        """Order the `Filterable`s in the container by an attribute with the given name, returning them as a new
        `FilterList` regardless of the type of filter container begun with.

        :param str attribute_name:
        :param bool reverse:
        :raise octue.exceptions.InvalidInputException: if an attribute with the given name doesn't exist on any of the
            container's members
        :return FilterList:
        """
        pass


def _filter(self, ignore_items_without_attribute=True, **kwargs):
    """Return a new instance containing only the Filterables to which the given filter criteria apply.

    :param bool ignore_items_without_attribute:
    :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the
            values to filter for
    :return octue.resources.filter_containers.FilterSet:
    """
    if any(not isinstance(item, Filterable) for item in self):
        raise TypeError(f"All items in a {type(self).__name__} must be of type {Filterable.__name__}.")

    raise_error_if_filter_is_invalid = not ignore_items_without_attribute

    if len(kwargs) == 1:
        return type(self)(
            (
                item
                for item in self
                if item.satisfies(raise_error_if_filter_is_invalid=raise_error_if_filter_is_invalid, **kwargs)
            )
        )

    filter_names = list(kwargs)

    for filter_name in filter_names:
        filter_value = kwargs.pop(filter_name)
        return _filter(self, raise_error_if_filter_is_invalid, **{filter_name: filter_value}).filter(**kwargs)


def _order_by(self, attribute_name, reverse=False):
    """Order the `Filterable`s in the container by an attribute with the given name, returning them as a new
    `FilterList` regardless of the type of filter container begun with.

    :param str attribute_name:
    :param bool reverse:
    :raise octue.exceptions.InvalidInputException: if an attribute with the given name doesn't exist on any of the
        container's members
    :return FilterList:
    """
    try:
        return FilterList(sorted(self, key=lambda item: getattr(item, attribute_name), reverse=reverse))
    except AttributeError:
        raise exceptions.InvalidInputException(
            f"An attribute named {attribute_name!r} does not exist on one or more members of {self!r}."
        )


class FilterSet(FilterContainer, set):
    filter = _filter
    order_by = _order_by


class FilterList(FilterContainer, list):
    filter = _filter
    order_by = _order_by


class FilterDict(FilterContainer, UserDict):
    def filter(self, ignore_items_without_attribute=True, **kwargs):
        """Return a new instance containing only the Filterables for which the given filter criteria apply are
        satisfied.

        :param bool ignore_items_without_attribute:
        :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the
            values to filter for
        :return FilterDict:
        """
        if any(not isinstance(item, Filterable) for item in self.values()):
            raise TypeError(f"All values in a {type(self).__name__} must be of type {Filterable.__name__}.")

        raise_error_if_filter_is_invalid = not ignore_items_without_attribute

        if len(kwargs) == 1:
            return type(self)(
                {
                    key: value
                    for key, value in self.items()
                    if value.satisfies(raise_error_if_filter_is_invalid=raise_error_if_filter_is_invalid, **kwargs)
                }
            )

        filter_names = list(kwargs)

        for filter_name in filter_names:
            filter_value = kwargs.pop(filter_name)
            return self.filter(raise_error_if_filter_is_invalid, **{filter_name: filter_value}).filter(**kwargs)

    def order_by(self, attribute_name, reverse=False):
        """Order the instance by the given attribute_name, returning the instance's elements as a new FilterList.

        :param str attribute_name: a dot-separated (optionally nested) attribute name e.g. "a", "a.b", "a.b.c"
        :param bool reverse:
        :raise octue.exceptions.InvalidInputException: if an attribute with the given name doesn't exist on any of the
            FilterDict's values
        :return FilterList:
        """
        try:
            return FilterList(
                sorted(self.values(), key=lambda item: get_nested_attribute(item, attribute_name), reverse=reverse)
            )

        except AttributeError:
            raise exceptions.InvalidInputException(
                f"An attribute named {attribute_name!r} does not exist on one or more members of {self!r}."
            )
