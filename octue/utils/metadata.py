import copy
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
    cached_metadata = _get_metadata_from_cache(absolute_path)

    if cached_metadata:
        return cached_metadata

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

    _overwrite_metadata_cache(absolute_path, local_metadata)
    return local_metadata


def overwrite_local_metadata_file(data, path=METADATA_FILENAME):
    """Create or overwrite the given local metadata file with the given data.

    :param dict data:
    :param str path:
    :return None:
    """
    absolute_path = os.path.abspath(path)
    cached_metadata = _get_metadata_from_cache(absolute_path)

    if data == cached_metadata:
        logger.info("Avoiding overwriting local metadata file - its data is already in sync with the cache.")
        return

    _overwrite_metadata_cache(absolute_path, data)

    with open(path, "w") as f:
        json.dump(data, f, cls=OctueJSONEncoder, indent=4)
        f.write("\n")


def _get_metadata_from_cache(absolute_path):
    """Get the metadata for the given local metadata file. If it's not cached, return `None`.

    :param str absolute_path: the path to the local metadata file
    :return dict|None: the metadata or, if the file hasn't been cached, `None`
    """
    logger.info("Using cached local metadata.")
    return copy.deepcopy(cached_local_metadata_files.get(absolute_path))


def _overwrite_metadata_cache(absolute_path, data):
    """Overwrite the metadata cache entry for the given local metadata file.

    :param str absolute_path: the path to the local metadata file
    :param dict data: the data to overwrite the existing cache entry with.
    :return None:
    """
    cached_local_metadata_files[absolute_path] = copy.deepcopy(data)
    logger.info("Updated local metadata cache.")
