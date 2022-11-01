import json

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


class Serialisable:
    """A mixin class to make instances serialisable as JSON.

    When calling ``Serialisable.serialise``, by default, only public attributes are included. If the class variable
    ``_SERIALISE_FIELDS`` is specified, then only these exact attributes are included (these can include non-public
    attributes); conversely, if ``_EXCLUDE_SERIALISE_FIELDS`` is specified and ``_SERIALISE_FIELDS`` is not, then all
    public attributes are included apart from the excluded ones.

    By default, JSON conversion is carried out using the ``OctueJSONEncoder``, which will sort keys as well as format
    and indent automatically. Additional keyword arguments will be passed to ``json.dumps`` to allow other formatting
    options.

    For example:

    .. code-block:: python

        class MyThing(Serialisable):

            _SERIALISE_FIELDS = ("a",)

            def __init__(self):
                self.a = 1
                self.b = 2
                self._private = 3
                self.__protected = 4

        MyThing().to_primitive()
        >>> {"a": 1}

    """

    _SERIALISE_FIELDS = None
    _EXCLUDE_SERIALISE_FIELDS = tuple()

    @classmethod
    def deserialise(cls, serialised_object, from_string=False):
        """Deserialise the given JSON-serialised object into an instance of the class.

        :param str|dict serialised_object: the string or dictionary of python primitives to deserialise into an instance
        :param bool from_string: if ``True``, deserialise from a JSON string; otherwise, deserialise from a dictionary
        :return any: an instance of the class
        """
        if from_string:
            serialised_object = json.loads(serialised_object, cls=OctueJSONDecoder)

        return cls(**serialised_object)

    @classmethod
    def from_file(cls, path, **kwargs):
        """Deserialise an instance from the given file.

        :param str path: the path to the JSON file containing the serialised instance
        :param kwargs: kwargs to pass in to the JSON deserialisation
        :return any: an instance of the class
        """
        with open(path) as f:
            return cls.deserialise(json.load(f, cls=OctueJSONDecoder, **kwargs))

    def serialise(self, **kwargs):
        """Serialise the instance to a JSON string of primitives. See the ``Serialisable`` constructor for more
        information.

        :param kwargs: kwargs to pass in to the JSON serialisation
        :return str: a JSON string containing the instance as a serialised python primitive
        """
        return json.dumps(self.to_primitive(), cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)

    def to_primitive(self):
        """Convert the instance into a JSON-compatible python dictionary of its attributes as primitives. See the
        ``Serialisable`` constructor for more information.

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

    def to_file(self, path, **kwargs):
        """Write the instance to a JSON file.

        :param str path: path of file to write to, including relative or absolute path and .json extension
        :param kwargs: kwargs to pass in to the JSON serialisation
        :return None:
        """
        with open(path, "w") as f:
            f.write(self.serialise(**kwargs))
