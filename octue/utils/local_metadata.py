import json
import os

from octue.utils.decoders import OctueJSONDecoder


LOCAL_METADATA_FILENAME = ".octue"


def load_local_metadata_file(path):
    """Load metadata from a local metadata records file, returning an empty dictionary if the file does not exist or is
    incorrectly formatted.

    :return dict:
    """
    if not os.path.exists(path):
        return {}

    with open(path) as f:
        try:
            return json.load(f, cls=OctueJSONDecoder)
        except json.decoder.JSONDecodeError:
            return {}
