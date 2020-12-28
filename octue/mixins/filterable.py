import functools


BASE_FILTERS = (
    ("icontains", lambda item, filter_value: filter_value.lower() in item.lower()),
    ("contains", lambda item, filter_value: filter_value in item),
    ("ends_with", lambda item, filter_value: item.endswith(filter_value)),
    ("starts_with", lambda item, filter_value: item.startswith(filter_value)),
    ("exact", lambda item, filter_value: filter_value in item),
    ("notnone", lambda item, filter_value: item is not None),
)


class Filterable:

    _ATTRIBUTES_TO_FILTER_BY = None

    def __init__(self, filters=None, *args, **kwargs):

        if not isinstance(self._ATTRIBUTES_TO_FILTER_BY, tuple) or len(self._ATTRIBUTES_TO_FILTER_BY) == 0:
            raise AttributeError(
                "The '_ATTRIBUTES_TO_FILTER_BY' attribute of Filterable subclasses must specify which attributes to "
                "filter by as a non-zero length tuple."
            )

        self._filters = filters or self._build_filters()
        super().__init__(*args, **kwargs)

    def filter(self, filter_name=None, filter_value=None):

        if filter_name not in self._filters:
            raise ValueError(f"Filtering by {filter_name} is not currently supported.")

        filtered_attribute_name, filter_ = self._filters[filter_name]

        filtered_items = [
            item for item in self._get_nested_attribute(filtered_attribute_name) if filter_(item, filter_value)
        ]

        # instance_attributes = vars(self).copy()
        # del instance_attributes[filtered_attribute_name]
        return self.__class__(**{filtered_attribute_name: filtered_items})

    def _build_filters(self):

        filters = {}

        for attribute_name in self._ATTRIBUTES_TO_FILTER_BY:

            for base_filter_name, filter_ in BASE_FILTERS:
                filter_name = f"{attribute_name.strip('s_')}__{base_filter_name}"
                filters[filter_name] = (attribute_name, filter_)

        return filters

    def _get_nested_attribute(self, attribute_name):
        return functools.reduce(getattr, attribute_name.split("."), self)
