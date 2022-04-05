import json
import logging
import os

from octue.utils.decoders import OctueJSONDecoder


logger = logging.getLogger(__name__)


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
            logger.warning(
                f"The {LOCAL_METADATA_FILENAME!r} metadata file at {path!r} is incorrectly formatted so no metadata "
                f"can be read from it. Please fix or delete it."
            )
            return {}
