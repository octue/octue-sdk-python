import json
from json import JSONDecoder

import dateutil.parser


class OctueJSONDecoder(JSONDecoder):
    """A JSON Decoder to convert default json objects into their Datafile, Dataset or Manifest classes as appropriate"""

    def __init__(self, *args, object_hook=None, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=object_hook or self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        """Transform the object from a JSON-compatible python primitive into another python object, including ones that
        are not directly JSON-compatible.

        :param any obj: any JSON-compatible python primitive
        :return any: any python object
        """
        if "_type" not in obj:
            return obj

        if obj["_type"] == "set":
            return set(obj["items"])

        if obj["_type"] == "datetime":
            return dateutil.parser.parse(obj["value"])

        return obj
