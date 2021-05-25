import collections.abc
import numbers

from octue import exceptions
from octue.utils.objects import get_nested_attribute


IS_FILTER_ACTIONS = {
    "is": lambda item, filter_value: item is filter_value,
    "is_not": lambda item, filter_value: item is not filter_value,
}

EQUALS_FILTER_ACTIONS = {
    "equals": lambda item, filter_value: filter_value == item,
    "not_equals": lambda item, filter_value: filter_value != item,
}

COMPARISON_FILTER_ACTIONS = {
    "lt": lambda item, filter_value: item < filter_value,
    "lte": lambda item, filter_value: item <= filter_value,
    "gt": lambda item, filter_value: item > filter_value,
    "gte": lambda item, filter_value: item >= filter_value,
}

CONTAINS_FILTER_ACTIONS = {
    "contains": lambda item, filter_value: filter_value in item,
    "not_contains": lambda item, filter_value: filter_value not in item,
}

ICONTAINS_FILTER_ACTIONS = {
    "icontains": lambda item, filter_value: filter_value.casefold() in item.casefold(),
    "not_icontains": lambda item, filter_value: filter_value.casefold() not in item.casefold(),
}


# Filters for specific types e.g. list or int.
TYPE_FILTERS = {
    "bool": IS_FILTER_ACTIONS,
    "str": {
        "iequals": lambda item, filter_value: filter_value.casefold() == item.casefold(),
        "not_iequals": lambda item, filter_value: filter_value.casefold() != item.casefold(),
        "starts_with": lambda item, filter_value: item.startswith(filter_value),
        "not_starts_with": lambda item, filter_value: not item.startswith(filter_value),
        "ends_with": lambda item, filter_value: item.endswith(filter_value),
        "not_ends_with": lambda item, filter_value: not item.endswith(filter_value),
        **EQUALS_FILTER_ACTIONS,
        **COMPARISON_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
        **CONTAINS_FILTER_ACTIONS,
        **ICONTAINS_FILTER_ACTIONS,
    },
    "NoneType": IS_FILTER_ACTIONS,
    "LabelSet": {
        "any_label_contains": lambda item, filter_value: item.any_label_contains(filter_value),
        "not_any_label_contains": lambda item, filter_value: not item.any_label_contains(filter_value),
        "any_label_starts_with": lambda item, filter_value: item.any_label_starts_with(filter_value),
        "not_any_label_starts_with": lambda item, filter_value: not item.any_label_starts_with(filter_value),
        "any_label_ends_with": lambda item, filter_value: item.any_label_ends_with(filter_value),
        "not_any_label_ends_with": lambda item, filter_value: not item.any_label_ends_with(filter_value),
        **EQUALS_FILTER_ACTIONS,
        **CONTAINS_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
    },
}

# Filters for interfaces e.g. iterables or numbers.
INTERFACE_FILTERS = {
    numbers.Number: {**EQUALS_FILTER_ACTIONS, **COMPARISON_FILTER_ACTIONS, **IS_FILTER_ACTIONS},
    collections.abc.Iterable: {
        **EQUALS_FILTER_ACTIONS,
        **CONTAINS_FILTER_ACTIONS,
        **ICONTAINS_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
    },
}


class Filterable:
    def satisfies(self, raise_error_if_filter_is_invalid=True, **kwargs):
        """Check that the instance satisfies the given filter for the given filter value. The filter should be provided
        as a single keyword argument such as `name__first__equals="Joe"`

        :param bool raise_error_if_filter_is_invalid:
        :param {str: any} kwargs: a single keyword argument whose key is the name of the filter and whose value is the
            value to filter for
        :return mixed:
        """
        if len(kwargs) != 1:
            raise ValueError(f"The satisfies method only takes one keyword argument; received {kwargs!r}.")

        filter_name, filter_value = list(kwargs.items())[0]

        attribute_name, filter_action = self._split_filter_name(filter_name)

        try:
            attribute = get_nested_attribute(self, attribute_name)

        except AttributeError as error:
            if raise_error_if_filter_is_invalid:
                raise error
            return False

        filter_ = self._get_filter(attribute, filter_action)

        return filter_(attribute, filter_value)

    def _split_filter_name(self, filter_name):
        """Split the filter name into the attribute name and filter action, raising an error if it the attribute name
        and filter action aren't delimited by a double underscore i.e. "__".
        """
        *attribute_names, filter_action = filter_name.split("__")

        if not attribute_names:
            raise exceptions.InvalidInputException(
                f"Invalid filter name {filter_name!r}. Filter names should be in the form "
                f"'<attribute_name_0>__<attribute_name_1>__<...>__<filter_kind>' with at least one attribute name "
                f"included."
            )

        return ".".join(attribute_names), filter_action

    def _get_filter(self, attribute, filter_action):
        """Get the filter for the attribute and filter action, raising an error if there is no filter action of that
        name.
        """
        try:
            return self._get_filter_actions_for_attribute(attribute)[filter_action]

        except KeyError as error:
            raise exceptions.InvalidInputException(
                f"There is no filter called {error.args[0]!r} for attributes of type {type(attribute)}. The options "
                f"are {self._get_filter_actions_for_attribute(attribute).keys()!r}"
            )

    def _get_filter_actions_for_attribute(self, attribute):
        """Get the possible filters for the given attribute based on its type or interface, raising an error if the
        attribute's type isn't supported (i.e. if there aren't any filters defined for it)."""
        try:
            return TYPE_FILTERS[type(attribute).__name__]

        except KeyError as error:
            # This allows handling of objects that conform to a certain interface (e.g. iterables) without needing the
            # specific type.
            for type_ in INTERFACE_FILTERS:
                if not isinstance(attribute, type_):
                    continue
                return INTERFACE_FILTERS[type_]

            raise exceptions.InvalidInputException(
                f"Attributes of type {error.args[0]} are not currently supported for filtering."
            )
