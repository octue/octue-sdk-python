import functools


BASE_FILTERS = (
    ("icontains", lambda filter_value, item: filter_value.lower() in item.lower()),
    ("contains", lambda filter_value, item: filter_value in item),
    ("ends_with", lambda filter_value, item: item.endswith(filter_value)),
    ("starts_with", lambda filter_value, item: item.startswith(filter_value)),
    ("exact", lambda filter_value, item: filter_value in item),
    ("notnone", lambda filter_value, item: item is not None),
)


class FilteredSet:
    def __init__(self, iterable):
        self._iterable = iterable

    def __repr__(self):
        return f"<{type(self).__name__}(iterable={self._iterable!r}>"

    def __iter__(self):
        yield from self._iterable


class Filterable:

    _ATTRIBUTES_TO_FILTER_BY = None

    def __init__(self, *args, **kwargs):

        if not isinstance(self._ATTRIBUTES_TO_FILTER_BY, tuple) or len(self._ATTRIBUTES_TO_FILTER_BY) == 0:
            raise AttributeError(
                "The '_ATTRIBUTES_TO_FILTER_BY' attribute of Filterable subclasses must specify which attributes to "
                "filter by as a non-zero length tuple."
            )

        self._filters = self._build_filters()
        super().__init__(*args, **kwargs)

    def filter(self, filter_name=None, filter_value=None):

        if filter_name not in self._filters:
            raise ValueError(f"Filtering by {filter_name} is not currently supported.")

        attribute_name, filter_ = self._filters[filter_name]

        return FilteredSet({item for item in self._get_nested_attribute(attribute_name) if filter_(filter_value, item)})

    def _build_filters(self):
        filters = {}

        for attribute_name in self._ATTRIBUTES_TO_FILTER_BY:

            for base_filter_name, filter_ in BASE_FILTERS:
                filter_name = f"{attribute_name.strip('s_')}__{base_filter_name}"
                filters[filter_name] = (attribute_name, filter_)

        return filters

    def _get_nested_attribute(self, attribute_name):
        return functools.reduce(getattr, attribute_name.split("."), self)
