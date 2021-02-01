import collections.abc
import numbers

from octue import exceptions


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
    "icontains": lambda item, filter_value: filter_value.lower() in item.lower(),
    "not_icontains": lambda item, filter_value: filter_value.lower() not in item.lower(),
}


# Filters for specific types e.g. list or int.
TYPE_FILTERS = {
    "bool": IS_FILTER_ACTIONS,
    "str": {
        "iequals": lambda item, filter_value: filter_value.lower() == item.lower(),
        "not_iequals": lambda item, filter_value: filter_value.lower() != item.lower(),
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
    "TagSet": {
        "any_tag_contains": lambda item, filter_value: item.any_tag_contains(filter_value),
        "not_any_tag_contains": lambda item, filter_value: not item.any_tag_contains(filter_value),
        "any_tag_starts_with": lambda item, filter_value: item.any_tag_starts_with(filter_value),
        "not_any_tag_starts_with": lambda item, filter_value: not item.any_tag_starts_with(filter_value),
        "any_tag_ends_with": lambda item, filter_value: item.any_tag_ends_with(filter_value),
        "not_any_tag_ends_with": lambda item, filter_value: not item.any_tag_ends_with(filter_value),
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
    def satisfies(self, filter_name, filter_value):
        """ Check that the instance satisfies the given filter for the given filter value. """
        attribute_name, filter_action = self._split_filter_name(filter_name)

        try:
            attribute = getattr(self, attribute_name)
        except AttributeError:
            raise AttributeError(f"An attribute named {attribute_name!r} does not exist on {self!r}.")

        filter_ = self._get_filter(attribute, filter_action)
        return filter_(attribute, filter_value)

    def _split_filter_name(self, filter_name):
        """Split the filter name into the attribute name and filter action, raising an error if it the attribute name
        and filter action aren't delimited by a double underscore i.e. "__".
        """
        try:
            attribute_name, filter_action = filter_name.split("__", 1)
        except ValueError:
            raise exceptions.InvalidInputException(
                f"Invalid filter name {filter_name!r}. Filter names should be in the form "
                f"'<attribute_name>__<filter_kind>'."
            )

        return attribute_name, filter_action

    def _get_filter(self, attribute, filter_action):
        """Get the filter for the attribute and filter action, raising an error if there is no filter action of that
        name.
        """
        try:
            return self._get_filter_actions_for_attribute(attribute)[filter_action]

        except KeyError as error:
            attribute_type = type(attribute)
            raise exceptions.InvalidInputException(
                f"There is no filter called {error.args[0]!r} for attributes of type {attribute_type}. The options "
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
