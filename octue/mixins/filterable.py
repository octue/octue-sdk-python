import functools

from octue import exceptions


BASE_FILTERS = (
    ("icontains", lambda item, filter_value: filter_value.lower() in item.lower()),
    ("contains", lambda item, filter_value: filter_value in item),
    ("ends_with", lambda item, filter_value: item.endswith(filter_value)),
    ("starts_with", lambda item, filter_value: item.startswith(filter_value)),
    ("exact", lambda item, filter_value: filter_value == item),
    ("notnone", lambda item, filter_value: item is not None),
)


class Filterable:

    _FILTERABLE_ATTRIBUTES = None

    def __init__(self, filters=None, *args, **kwargs):
        """ Instantiate a Filterable instance. The `filters` parameter should be a dictionary in this form:
        {<name_of_filter>: (name_of_attribute_to_filter, <boolean function with `item` and `filter_value` arguments>)}

        Any number of filters can be provided. These filters can then be called using their names in the `filter`
        method.
        """
        if not isinstance(self._FILTERABLE_ATTRIBUTES, tuple) or len(self._FILTERABLE_ATTRIBUTES) == 0:
            raise AttributeError(
                "The '_FILTERABLE_ATTRIBUTES' attribute of Filterable subclasses must specify which attributes to "
                "filter by as a non-zero length tuple."
            )

        filters = filters or {}
        self._filters = {**self._build_base_filters(), **filters}
        super().__init__(*args, **kwargs)

    def filter(self, filter_name=None, filter_value=None):
        """ Filter the instance using the filter function identified by `filter_name` parametrised by the desired
        `filter_value`.
        """
        if filter_name not in self._filters:
            raise exceptions.InvalidInputException(f"Filtering by {filter_name} is not currently supported.")

        filtered_attribute_name, filter_ = self._filters[filter_name]

        filtered_items = [
            item for item in self._get_nested_attribute(filtered_attribute_name) if filter_(item, filter_value)
        ]

        other_instance_attributes = {
            name: attribute
            for name, attribute in vars(self).items()
            if name != filtered_attribute_name and not name.startswith("_")
        }

        # Instantiate new inheriting class instance with the relevant attribute filtered and the other attributes
        # unchanged.
        return self.__class__(**{filtered_attribute_name: filtered_items, **other_instance_attributes})

    def _build_base_filters(self):
        """ Build the standard set of base filters based on the given filterable attributes. """
        filters = {}

        for attribute_name in self._FILTERABLE_ATTRIBUTES:

            for base_filter_name, filter_ in BASE_FILTERS:
                filter_name = f"{attribute_name.strip('s_')}__{base_filter_name}"
                filters[filter_name] = (attribute_name, filter_)

        return filters

    def _get_nested_attribute(self, attribute_name):
        """ Get a nested attribute from the instance (e.g. self.my_attribute.its_attribute - `getattr` only supports
        first-level attribute getting).
        """
        return functools.reduce(getattr, attribute_name.split("."), self)
