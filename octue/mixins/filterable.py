import collections.abc
import numbers

from octue import exceptions


def generate_complementary_filters(name, func):
    """Use a filter to generate its complementary filter, then return them together mapped to their names in a
    dictionary. The complementary filter is named f"not_{name}" or, if the name is "is", "is_not".

    :param str name:
    :param callable func:
    :return dict:
    """
    filter_action = {name: func}

    if name == "is":
        not_filter_name = "is_not"
    else:
        not_filter_name = f"not_{name}"

    not_filter_action = {
        not_filter_name: lambda item, value: not action(item, value) for name, action in filter_action.items()
    }

    return {**filter_action, **not_filter_action}


IS_FILTER_ACTIONS = generate_complementary_filters("is", lambda item, value: item is value)
EQUALS_FILTER_ACTIONS = generate_complementary_filters("equals", lambda item, value: value == item)
CONTAINS_FILTER_ACTIONS = generate_complementary_filters("contains", lambda item, value: value in item)
IN_RANGE_FILTER_ACTIONS = generate_complementary_filters("in_range", lambda item, value: value[0] <= item <= value[1])

ICONTAINS_FILTER_ACTIONS = generate_complementary_filters(
    "icontains", lambda item, value: value.lower() in item.lower()
)

COMPARISON_FILTER_ACTIONS = {
    "lt": lambda item, value: item < value,
    "lte": lambda item, value: item <= value,
    "gt": lambda item, value: item > value,
    "gte": lambda item, value: item >= value,
}


# Filters for specific types e.g. list or int.
TYPE_FILTERS = {
    "bool": IS_FILTER_ACTIONS,
    "str": {
        **generate_complementary_filters("iequals", lambda item, value: value.lower() == item.lower()),
        **generate_complementary_filters("starts_with", lambda item, value: item.startswith(value)),
        **generate_complementary_filters("ends_with", lambda item, value: item.endswith(value)),
        **EQUALS_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
        **COMPARISON_FILTER_ACTIONS,
        **CONTAINS_FILTER_ACTIONS,
        **ICONTAINS_FILTER_ACTIONS,
        **IN_RANGE_FILTER_ACTIONS,
    },
    "NoneType": IS_FILTER_ACTIONS,
    "datetime": {
        **EQUALS_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
        **COMPARISON_FILTER_ACTIONS,
        **IN_RANGE_FILTER_ACTIONS,
        "year_equals": lambda item, value: item.year == value,
        "year_in": lambda item, value: item.year in value,
        "month_equals": lambda item, value: item.month == value,
        "month_in": lambda item, value: item.month in value,
        "day_equals": lambda item, value: item.day == value,
        "day_in": lambda item, value: item.day in value,
        "weekday_equals": lambda item, value: item.weekday() == value,
        "weekday_in": lambda item, value: item.weekday() in value,
        "iso_weekday_equals": lambda item, value: item.isoweekday() == value,
        "iso_weekday_in": lambda item, value: item.isoweekday() in value,
        "time_equals": lambda item, value: item.time() == value,
        "time_in": lambda item, value: item.time() in value,
        "hour_equals": lambda item, value: item.hour == value,
        "hour_in": lambda item, value: item.hour in value,
        "minute_equals": lambda item, value: item.minute == value,
        "minute_in": lambda item, value: item.minute in value,
        "second_equals": lambda item, value: item.second == value,
        "second_in": lambda item, value: item.second in value,
        "in_date_range": lambda item, value: value[0] <= item.date() <= value[1],
        "in_time_range": lambda item, value: value[0] <= item.time() <= value[1],
    },
    "TagSet": {
        **generate_complementary_filters("any_tag_contains", lambda item, value: item.any_tag_contains(value)),
        **generate_complementary_filters("any_tag_starts_with", lambda item, value: item.any_tag_starts_with(value)),
        **generate_complementary_filters("any_tag_ends_with", lambda item, value: item.any_tag_ends_with(value)),
        **EQUALS_FILTER_ACTIONS,
        **CONTAINS_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
    },
}

# Filters for interfaces e.g. iterables or numbers.
INTERFACE_FILTERS = {
    numbers.Number: {
        **EQUALS_FILTER_ACTIONS,
        **COMPARISON_FILTER_ACTIONS,
        **IS_FILTER_ACTIONS,
        **IN_RANGE_FILTER_ACTIONS,
    },
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
