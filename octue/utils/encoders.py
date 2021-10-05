import datetime
from collections import UserString

from twined.utils import TwinedEncoder


class OctueJSONEncoder(TwinedEncoder):
    """A JSON Encoder which allows objects having a `to_primitives` method to control their own conversion to primitives."""

    def default(self, obj):

        # If the class defines a `to_primitives` method, use it.
        if hasattr(obj, "to_primitives"):
            return obj.to_primitives()

        # Convert sets to sorted lists (JSON doesn't support sets).
        if isinstance(obj, set):
            return {"_type": "set", "items": sorted(obj)}

        if isinstance(obj, UserString):
            return str(obj)

        if isinstance(obj, datetime.datetime):
            return {"_type": "datetime", "value": obj.isoformat()}

        # Otherwise let the base class default method raise the TypeError.
        return TwinedEncoder.default(self, obj)
