import json
import logging
import os

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)


METADATA_FILENAME = ".octue"


def load_local_metadata_file(path=METADATA_FILENAME):
    """Load metadata from a local metadata records file, returning an empty dictionary if the file does not exist or is
    incorrectly formatted.

    :param str path:
    :return dict:
    """
    if not os.path.exists(path):
        return {}

    with open(path) as f:
        try:
            return json.load(f, cls=OctueJSONDecoder)
        except json.decoder.JSONDecodeError:
            logger.warning(
                f"The metadata file at {path!r} is incorrectly formatted so no metadata can be read from it. Please "
                "fix or delete it."
            )
            return {}


def overwrite_local_metadata_file(data, path=METADATA_FILENAME):
    """Create or overwrite the given local metadata file with the given data.

    :param dict data:
    :param str path:
    :return None:
    """
    with open(path, "w") as f:
        json.dump(data, f, cls=OctueJSONEncoder, indent=4)
        f.write("\n")
