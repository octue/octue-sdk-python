import json

from octue.utils.encoders import OctueJSONEncoder


class Serialisable:
    """ Mixin class to make resources serialisable to JSON.

    Objects must have a `.logger` and a `.id` property

    """

    def __init__(self, *args, **kwargs):
        """ Constructor for serialisable mixin
        """
        # Ensure it passes construction argumnets up the chain
        super().__init__(*args, **kwargs)

    _serialise_fields = None

    def to_file(self, file_name, **kwargs):
        """ Write to a JSON file

        :parameter file_name:  file to write to, including relative or absolute path and .json extension
        :type file_name: path-like
        """
        self.logger.debug("Writing %s %s to file %s", self.__class__.__name__, self.id, file_name)
        with open(file_name, "w") as fp:
            fp.write(self.serialise(**kwargs, to_string=True))

    def serialise(self, to_string=False, **kwargs):
        """ Serialise into a primitive dict or JSON string

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

        :return: json string or dict contianing a serialised / primitive version of the resource.
        :rtype: str, dict
        """
        self.logger.debug("Serialising %s %s", self.__class__.__name__, self.id)

        # Get all non-private and non-protected attributes except for 'logger'
        attrs_to_serialise = self._serialise_fields or (
            k
            for k in self.__dir__()
            if ((k[:1] != "_") and (k != "logger") and (type(getattr(self, k, "")).__name__ != "method"))
        )
        self_as_primitive = {attr: getattr(self, attr, None) for attr in attrs_to_serialise}

        # TODO this conversion backward-and-forward is very inefficient but allows us to use the same encoder for
        #  converting the object to a dict as to strings, which ensures that nested attributes are also cast to
        #  primitive using their serialise() method. A more performant method would be to implement an encoder which
        #  returns python primitives, not strings. The reason we do this is to validate outbound information the same
        #  way as we validate incoming.
        string = json.dumps(self_as_primitive, cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)
        if to_string:
            return string

        return json.loads(string)
