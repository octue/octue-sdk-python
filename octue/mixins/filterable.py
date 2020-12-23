import functools


FILTERS = (
    ("icontains", lambda filter_value, attribute: filter_value.lower() in attribute.lower()),
    ("contains", lambda filter_value, attribute: filter_value in attribute.name),
    ("ends_with", lambda filter_value, attribute: attribute.endswith(filter_value)),
    ("starts_with", lambda filter_value, attribute: attribute.startswith(filter_value)),
    ("exact", lambda filter_value, attribute: filter_value in attribute),
    ("notnone", lambda filter_value, attribute: attribute is not None),
)


class Filterable:

    _ATTRIBUTES_TO_FILTER_BY = None

    def __init__(self, *args, **kwargs):

        if not isinstance(self._ATTRIBUTES_TO_FILTER_BY, tuple) or len(self._ATTRIBUTES_TO_FILTER_BY) == 0:
            raise AttributeError(
                "The '_ATTRIBUTES_TO_FILTER_BY' attribute of Filterable subclasses must specify which attributes to "
                "filter by as a non-zero length tuple."
            )

        self._filters = {}

        for attribute_name in self._ATTRIBUTES_TO_FILTER_BY:
            attribute = self._get_nested_attribute(attribute_name)

            for filter_name, filter_ in FILTERS:
                self._filters[f"{attribute_name}__{filter_name}"] = functools.partial(filter_, attribute=attribute)

        super().__init__(*args, **kwargs)

    def filter(self, filter_name=None, filter_value=None):

        if filter_name not in self._filters:
            raise ValueError(f"Filtering by {filter_name} is not currently supported.")

    def _get_nested_attribute(self, attribute_name):
        return functools.reduce(getattr, attribute_name.split("."), self)
