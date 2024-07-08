import copy
import json
import logging
import os

from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder


logger = logging.getLogger(__name__)


cached_local_metadata_files = {}
METADATA_FILENAME = ".octue"


class UpdateLocalMetadata:
    """A context manager that provides the contents of the given local metadata file and updates it with any changes
    made within its context. The local metadata is retrieved either from the disk or from the cache as appropriate.

    :param str path: the path to the local metadata. The file must be in JSON format.
    :return None:
    """

    def __init__(self, path=METADATA_FILENAME):
        self.path = path
        self._local_metadata = None

    def __enter__(self):
        """Load the local metadata file and return its contents.

        :return any: the contents of the local metadata file (converted from the JSON in the local metadata file)
        """
        self._local_metadata = load_local_metadata_file(self.path)
        return self._local_metadata

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Write any changes to the contents of the file.

        :return None:
        """
        overwrite_local_metadata_file(self._local_metadata, self.path)


def load_local_metadata_file(path=METADATA_FILENAME):
    """Load metadata from a local metadata records file, returning an empty dictionary if the file does not exist or is
    incorrectly formatted. If the file has already been cached, its contents are retrieved from the cache.

    :param str path: the path to the local metadata file
    :return dict: the contents of the local metadata file
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

    _overwrite_cache_entry(absolute_path, local_metadata)
    return local_metadata


def overwrite_local_metadata_file(data, path=METADATA_FILENAME):
    """Create or overwrite the given local metadata file with the given data. If the data to overwrite the file with is
    the same as the file's cache entry, no changes are made. If it's not, the cache entry is updated and the file is
    overwritten.

    :param dict data: the data to overwrite the local metadata file with
    :param str path: the path to the local metadata file
    :return None:
    """
    absolute_path = os.path.abspath(path)
    cached_metadata = _get_metadata_from_cache(absolute_path)

    if data == cached_metadata:
        logger.debug("Avoiding overwriting local metadata file - its data is already in sync with the cache.")
        return

    _overwrite_cache_entry(absolute_path, data)

    with open(path, "w") as f:
        json.dump(data, f, cls=OctueJSONEncoder, indent=4)
        f.write("\n")


def _get_metadata_from_cache(absolute_path):
    """Get the metadata for the given local metadata file from the cache. If it's not cached, return `None`.

    :param str absolute_path: the path to the local metadata file
    :return dict|None: the metadata or, if the file hasn't been cached, `None`
    """
    logger.debug("Using cached local metadata.")
    return copy.deepcopy(cached_local_metadata_files.get(absolute_path))


def _overwrite_cache_entry(absolute_path, data):
    """Overwrite the metadata cache entry for the given local metadata file.

    :param str absolute_path: the path to the local metadata file
    :param dict data: the data to overwrite the existing cache entry with.
    :return None:
    """
    cached_local_metadata_files[absolute_path] = copy.deepcopy(data)
    logger.debug("Updated local metadata cache.")
