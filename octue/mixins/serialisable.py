import json

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


class Serialisable:
    """Mixin class to make resources serialisable to JSON.

    The `logger` field is always excluded from serialisation if it is present.
    """

    _SERIALISE_FIELDS = None
    _EXCLUDE_SERIALISE_FIELDS = ("logger",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if "logger" not in self._EXCLUDE_SERIALISE_FIELDS:
            self._EXCLUDE_SERIALISE_FIELDS = (*self._EXCLUDE_SERIALISE_FIELDS, "logger")

    @classmethod
    def deserialise(cls, serialised_object, from_string=False):
        """Deserialise the given JSON-serialised object.

        :param str|dict serialised_object:
        :param bool from_string:
        :return any:
        """
        if from_string:
            serialised_object = json.loads(serialised_object, cls=OctueJSONDecoder)

        return cls(**serialised_object)

    def serialise(self, **kwargs):
        """Serialise to a JSON string of primitives. By default, only public attributes are included. If the class
        variable `_SERIALISE_FIELDS` is specified, then only these exact attributes are included (these can include
        non-public attributes); conversely, if `_EXCLUDE_SERIALISE_FIELDS` is specified and `_SERIALISE_FIELDS` is not,
        then all public attributes are included apart from the excluded ones. By default, the conversion is carried out
        using the `OctueJSONEncoder`, which will sort keys as well as format and indent automatically. Additional
        keyword arguments will be passed to ``json.dumps()`` to enable full override of formatting options.

        For example:
        ```
        class MyThing(Serialisable):
            _SERIALISE_FIELDS = ("a",)
            def __init__(self):
                self.a = 1
                self.b = 2
                self._private = 3
                self.__protected = 4

        MyThing().to_primitive()
        {"a": 1}
        ```

        :return str: JSON string containing a serialised primitive version of the resource
        """
        return json.dumps(self.to_primitive(), cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)

    def to_primitive(self):
        """Convert the instance into a JSON-compatible python dictionary of its attributes as primitives. The same rules
        as described in `serialise` apply.

        :return dict:
        """
        names_of_attributes_to_serialise = self._SERIALISE_FIELDS or (
            field_name
            for field_name in dir(self)
            if (
                field_name not in self._EXCLUDE_SERIALISE_FIELDS
                and (field_name[:1] != "_")
                and (type(getattr(self, field_name, "")).__name__ != "method")
            )
        )

        self_as_primitive = {}

        for name in names_of_attributes_to_serialise:
            attribute = getattr(self, name, None)

            # Serialise sets as sorted list (JSON doesn't support sets).
            if isinstance(attribute, set):
                self_as_primitive[name] = sorted(attribute)
            else:
                self_as_primitive[name] = attribute

        return self_as_primitive

    def to_file(self, filename, **kwargs):
        """Write the object to a JSON file.

        :param str filename: path of file to write to, including relative or absolute path and .json extension
        :return None:
        """
        with open(filename, "w") as f:
            f.write(self.serialise(**kwargs))
