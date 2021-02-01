from twined.utils import TwinedEncoder


class OctueJSONEncoder(TwinedEncoder):
    """A JSON Encoder which allows objects having a `serialise()` method to control their own conversion to primitives"""

    def default(self, obj):

        # If the class defines a serialise() method then use it
        if hasattr(obj, "serialise"):
            return obj.serialise()

        # Otherwise let the base class default method raise the TypeError
        return TwinedEncoder.default(self, obj)
