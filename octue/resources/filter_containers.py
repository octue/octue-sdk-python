from abc import ABC
from collections import UserDict

from octue import exceptions
from octue.mixins import Filterable
from octue.utils.objects import get_nested_attribute


class FilterContainer(ABC):
    def filter(self, ignore_items_without_attribute=True, **kwargs):
        """Return a new instance containing only the `Filterable`s to which the given filter criteria are `True`.

        :param bool ignore_items_without_attribute: if True, just ignore any members of the container without a filtered-for attribute rather than raising an error
        :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the values to filter for
        :return octue.resources.filter_containers.FilterContainer:
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
            return self.filter(raise_error_if_filter_is_invalid, **{filter_name: filter_value}).filter(**kwargs)

    def order_by(self, attribute_name, check_start_value=None, check_constant_increment=None, reverse=False):
        """Order the `Filterable`s in the container by an attribute with the given name, returning them as a new
        `FilterList` regardless of the type of filter container begun with.

        :param str attribute_name: name of attribute (optionally nested) to order by e.g. "a", "a.b", "a.b.c"
        :param any check_start_value: if provided, check that the first item in the ordered container has the given start value for the attribute ordered by
        :param int|float|None check_constant_increment: if given, check that the ordered-by attribute of each of the items in the ordered container increases by the given value when progressing along the sequence
        :param bool reverse: if True, reverse the ordering
        :raise octue.exceptions.InvalidInputException: if an attribute with the given name doesn't exist on any of the container's members
        :return FilterList:
        """
        attribute_name = ".".join(attribute_name.split("__"))

        try:
            results = FilterList(
                sorted(self, key=lambda item: get_nested_attribute(item, attribute_name), reverse=reverse)
            )

        except AttributeError:
            raise exceptions.InvalidInputException(
                f"An attribute named {attribute_name!r} does not exist on one or more members of {self!r}."
            )

        if check_start_value is not None:
            if get_nested_attribute(results[0], attribute_name) != check_start_value:
                raise exceptions.BrokenSequenceException(
                    f"The attribute {attribute_name!r} of the first item of {results!r} does equal the given start "
                    f"value {check_start_value!r}."
                )

        if check_constant_increment is not None:
            required_increment = check_constant_increment

            for i in range(len(results) - 1):
                actual_increment = get_nested_attribute(results[i + 1], attribute_name) - get_nested_attribute(
                    results[i], attribute_name
                )

                if actual_increment != required_increment:
                    raise exceptions.BrokenSequenceException(
                        f"The attributes {attribute_name!r} of the items of {results!r} do not increase by a constant "
                        f"increment of {required_increment}."
                    )

        return results

    def one(self, **kwargs):
        """If a single result exists for the given filters, return it. Otherwise, raise an error.

        :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the values to filter for
        :raise octue.exceptions.UnexpectedNumberOfResultsException: if zero or more than one results satisfy the filters
        :return octue.resources.mixins.filterable.Filterable:
        """
        results = self.filter(**kwargs)

        if len(results) > 1:
            raise exceptions.UnexpectedNumberOfResultsException(f"More than one result found for filters {kwargs}.")

        if len(results) == 0:
            raise exceptions.UnexpectedNumberOfResultsException(f"No results found for filters {kwargs}.")

        if isinstance(self, UserDict):
            return results.popitem()

        return results.pop()


class FilterSet(FilterContainer, set):
    pass


class FilterList(FilterContainer, list):
    pass


class FilterDict(FilterContainer, UserDict):
    def filter(self, ignore_items_without_attribute=True, **kwargs):
        """Return a new instance containing only the Filterables for which the given filter criteria apply are
        satisfied.

        :param bool ignore_items_without_attribute: if True, just ignore any members of the container without a filtered-for attribute rather than raising an error
        :param {str: any} kwargs: keyword arguments whose keys are the name of the filter and whose values are the values to filter for
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

        :param str attribute_name: name of attribute (optionally nested) to order by e.g. "a", "a.b", "a.b.c"
        :param bool reverse: if True, reverse the ordering
        :raise octue.exceptions.InvalidInputException: if an attribute with the given name doesn't exist on any of the FilterDict's values
        :return FilterList:
        """
        try:
            return FilterList(
                sorted(self.items(), key=lambda item: get_nested_attribute(item[1], attribute_name), reverse=reverse)
            )

        except AttributeError:
            raise exceptions.InvalidInputException(
                f"An attribute named {attribute_name!r} does not exist on one or more members of {self!r}."
            )
