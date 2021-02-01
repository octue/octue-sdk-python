from json import JSONDecoder

from octue.resources import Datafile, Dataset, Manifest


def default_object_hook(obj):
    """A hook to convert default json objects into their Datafile, Dataset or Manifest class as appropriate"""

    # object hooks are called whenever a json object is created. When nested, this is done from innermost (deepest
    # nesting) out so it's safe to work at multiple levels here

    if "files" in obj:
        files = [Datafile(**df) for df in obj.pop("files")]
        return {**obj, "files": files}

    if "datasets" in obj:
        datasets = [Dataset(**ds) for ds in obj.pop("datasets")]
        return Manifest(**obj, datasets=datasets)

    return obj


class OctueJSONDecoder(JSONDecoder):
    """A JSON Decoder to convert default json objects into their Datafile, Dataset or Manifest classes as appropriate"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_hook = self.object_hook or default_object_hook
