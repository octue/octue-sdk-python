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

    def to_file(self, file_name, **kwargs):
        """Write the object to a JSON file.

        :parameter str file_name: file to write to, including relative or absolute path and .json extension
        :return None:
        """
        with open(file_name, "w") as fp:
            fp.write(self.serialise(**kwargs, to_string=True))

    def serialise(self, to_string=False, **kwargs):
        """Serialise into a primitive dict or JSON string.

        Serialises all non-private and non-protected attributes except for 'logger', unless the subclass has a
        `_serialise_fields` tuple of the attribute names to serialise. For example:
        ```
        class MyThing(Serialisable):
            _serialise_fields = ("a",)
            def __init__(self):
                self.a = 1
                self.b = 2

        MyThing().serialise()
        {"a": 1}
        ```

        By default, serialises using the OctueJSONEncoder, and will sort keys as well as format and indent
        automatically. Additional keyword arguments will be passed to ``json.dumps()`` to enable full override
        of formatting options

        :return: json string or dict containing a serialised/primitive version of the resource.
        :rtype: str, dict
        """
        # Get all non-private and non-protected attributes except those excluded specifically
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

        # TODO this conversion backward-and-forward is very inefficient but allows us to use the same encoder for
        #  converting the object to a dict as to strings, which ensures that nested attributes are also cast to
        #  primitive using their serialise() method. A more performant method would be to implement an encoder which
        #  returns python primitives, not strings. The reason we do this is to validate outbound information the same
        #  way as we validate incoming.
        string = json.dumps(self_as_primitive, cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)

        if to_string:
            return string

        return json.loads(string, cls=OctueJSONDecoder)
