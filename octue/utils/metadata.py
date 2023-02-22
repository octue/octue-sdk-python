import json
import logging
import os

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)


cached_local_metadata_files = {}
METADATA_FILENAME = ".octue"


def load_local_metadata_file(path=METADATA_FILENAME):
    """Load metadata from a local metadata records file, returning an empty dictionary if the file does not exist or is
    incorrectly formatted.

    :param str path:
    :return dict:
    """
    absolute_path = os.path.abspath(path)

    try:
        cached_metadata = cached_local_metadata_files[absolute_path]
        logger.info("Using cached local metadata.")
        return cached_metadata
    except KeyError:
        pass

    if not os.path.exists(path):
        local_metadata = {}

    else:
        with open(path) as f:
            try:
                local_metadata = json.load(f, cls=OctueJSONDecoder)
            except json.decoder.JSONDecodeError:
                logger.warning(
                    f"The metadata file at {path!r} is incorrectly formatted so no metadata can be read from it. "
                    "Please fix or delete it."
                )
                local_metadata = {}

    cached_local_metadata_files[absolute_path] = local_metadata
    return local_metadata


def overwrite_local_metadata_file(data, path=METADATA_FILENAME):
    """Create or overwrite the given local metadata file with the given data.

    :param dict data:
    :param str path:
    :return None:
    """
    cached_metadata = cached_local_metadata_files.get(os.path.abspath(path))

    if data == cached_metadata:
        logger.info("Avoiding overwriting local metadata file as it's the same as what's cached.")
        return

    cached_local_metadata_files[os.path.abspath(path)] = data
    logger.info("Updated local metadata cache.")

    with open(path, "w") as f:
        json.dump(data, f, cls=OctueJSONEncoder, indent=4)
        f.write("\n")
