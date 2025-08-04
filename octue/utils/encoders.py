from collections import UserString
import datetime
import importlib.util
import json

# Determines whether numpy is available
_numpy_spec = importlib.util.find_spec("numpy")


class OctueJSONEncoder(json.JSONEncoder):
    """A JSON encoder which allows objects having a `to_primitive` method to control their own conversion to python
    primitives. It also serialises:
    - sets
    - UserStrings
    - datetime.datetime instances
    - numpy arrays, ndarrays, and matrices

    This is designed to work "out of the box" to help people serialise the outputs from twined applications. It does not
    require installation of numpy - it'll work fine if numpy is not present, so can be used in a versatile tool in
    uncertain environments.

    Example use:
    ```
    from octue.utils.encoders import OctueJSONEncoder
    some_json = {"a": np.array([0, 1])}
    json.dumps(some_json, cls=OctueJSONEncoder)
    ```
    """

    def default(self, obj):
        """Transform the object into a JSON-compatible python primitive.

        :param any obj: any python object
        :return any: a JSON-compatible python primitive
        """
        # If the class defines a `to_primitive` method, use it.
        if hasattr(obj, "to_primitive"):
            return obj.to_primitive()

        # Convert sets to sorted lists (JSON doesn't support sets).
        if isinstance(obj, set):
            return {"_type": "set", "items": sorted(obj)}

        if isinstance(obj, UserString):
            return str(obj)

        if isinstance(obj, datetime.datetime):
            return {"_type": "datetime", "value": obj.isoformat()}

        if _numpy_spec is not None:
            import numpy

            if isinstance(obj, numpy.ndarray) or isinstance(obj, numpy.matrix):
                return obj.tolist()

        # Otherwise let the base class default method raise the TypeError.
        return json.JSONEncoder.default(self, obj)
