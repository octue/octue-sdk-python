import collections

from octue import exceptions


FILTERS = {
    bool: {"is": lambda item, filter_value: item is filter_value},
    str: {
        "icontains": lambda item, filter_value: filter_value.lower() in item.lower(),
        "contains": lambda item, filter_value: filter_value in item,
        "ends_with": lambda item, filter_value: item.endswith(filter_value),
        "starts_with": lambda item, filter_value: item.startswith(filter_value),
        "equals": lambda item, filter_value: filter_value == item,
    },
    type(None): {
        "none": lambda item, filter_value: item is None,
        "not_none": lambda item, filter_value: item is not None,
    },
    collections.Iterable: {
        "contains": lambda item, filter_value: filter_value in item,
        "not_contains": lambda item, filter_value: filter_value not in item,
        "starts_with": lambda item, filter_value: item.starts_with(filter_value),
        "ends_with": lambda item, filter_value: item.ends_with(filter_value),
    },
}


class Filteree:

    _FILTERABLE_ATTRIBUTES = None

    def check_attribute(self, filter_name, filter_value):
        if self._FILTERABLE_ATTRIBUTES is None:
            raise ValueError(
                "A Filteree should have at least one attribute name in its class-level _FILTERABLE_ATTRIBUTES"
            )

        try:
            attribute_name, filter_action = filter_name.split("__", 1)
        except ValueError:
            raise exceptions.InvalidInputException(
                f"Invalid field lookup '{filter_name}'. Filter names should be in the form "
                f"'<attribute_name>__<filter_kind>"
            )

        attribute = getattr(self, attribute_name)
        filter_ = self._get_filter(attribute)[filter_action]
        return filter_(attribute, filter_value)

    def _get_filter(self, attribute):
        try:
            return FILTERS[type(attribute)]

        except KeyError as error:
            for type_ in FILTERS:
                if not isinstance(attribute, type_):
                    continue
                return FILTERS[type_]

            raise error
