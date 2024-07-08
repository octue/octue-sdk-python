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
    """A context manager that provides the contents of the given dataset's or datafile's local metadata file and updates
    it with any changes made within its context. The local metadata is retrieved either from the disk or from the cache
    as appropriate.

    :param octue.resources.datafile.Datafile|octue.resources.dataset.Dataset datafile_or_dataset: the datafile or dataset to update the local metadata for
    :return None:
    """

    def __init__(self, datafile_or_dataset):
        self.datafile_or_dataset = datafile_or_dataset
        self._local_metadata = None

    def __enter__(self):
        """Load the local metadata file and return its contents.

        :return any: the contents of the local metadata file (converted from the JSON in the local metadata file)
        """
        self._local_metadata = load_local_metadata_file(self.datafile_or_dataset)
        return self._local_metadata

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Write any changes to the contents of the file.

        :return None:
        """
        overwrite_local_metadata_file(self._local_metadata, self.datafile_or_dataset)


def load_local_metadata_file(datafile_or_dataset):
    """Load metadata from the local metadata records file for a datafile or dataset, returning an empty dictionary if
    the file does not exist or is incorrectly formatted. If the file has already been cached, its contents are retrieved
    from the cache.

    :param octue.resources.datafile.Datafile|octue.resources.dataset.Dataset datafile_or_dataset: the datafile or dataset to load the local metadata file for
    :return dict: the contents of the local metadata file
    """
    cached_metadata = _get_metadata_from_cache(datafile_or_dataset)

    if cached_metadata:
        return cached_metadata

    if not os.path.exists(datafile_or_dataset.metadata_path):
        local_metadata = {}

    else:
        with open(datafile_or_dataset.metadata_path) as f:
            try:
                local_metadata = json.load(f, cls=OctueJSONDecoder)
            except json.decoder.JSONDecodeError:
                logger.warning(
                    f"The metadata file at {datafile_or_dataset.metadata_path!r} is incorrectly formatted so no "
                    f"metadata can be read from it. Please fix or delete it."
                )
                local_metadata = {}

    _overwrite_cache_entry(datafile_or_dataset, local_metadata)
    return local_metadata


def overwrite_local_metadata_file(data, datafile_or_dataset):
    """Create or overwrite the local metadata file of a datafile or dataset with the given data. If the data to
    overwrite the file with is the same as the file's cache entry, no changes are made. If it's not, the cache entry is
    updated and the file is overwritten.

    :param dict data: the data to overwrite the local metadata file with
    :param octue.resources.datafile.Datafile|octue.resources.dataset.Dataset datafile_or_dataset: the datafile or dataset to overwrite the local metadata file for
    :return None:
    """
    cached_metadata = _get_metadata_from_cache(datafile_or_dataset)

    if data == cached_metadata:
        logger.debug("Avoiding overwriting local metadata file - its data is already in sync with the cache.")
        return

    _overwrite_cache_entry(datafile_or_dataset, data)

    with open(datafile_or_dataset.metadata_path, "w") as f:
        json.dump(data, f, cls=OctueJSONEncoder, indent=4)
        f.write("\n")


def _get_metadata_from_cache(datafile_or_dataset):
    """Get metadata for a datafile or dataset from the cache. If it's not cached, return `None`.

    :param octue.resources.datafile.Datafile|octue.resources.dataset.Dataset datafile_or_dataset: the datafile or dataset to get metadata from the cache for
    :return dict|None: the metadata or, if the file hasn't been cached, `None`
    """
    logger.debug("Using cached local metadata for %r.", datafile_or_dataset)
    return copy.deepcopy(cached_local_metadata_files.get(datafile_or_dataset.id))


def _overwrite_cache_entry(datafile_or_dataset, data):
    """Overwrite the metadata cache entry for a datafile or dataset.

    :param octue.resources.datafile.Datafile|octue.resources.dataset.Dataset datafile_or_dataset: the datafile or dataset to overwrite metadata in the cache for
    :param dict data: the data to overwrite the existing cache entry with.
    :return None:
    """
    cached_local_metadata_files[datafile_or_dataset.id] = copy.deepcopy(data)
    logger.debug("Updated local metadata cache for %r.", datafile_or_dataset)
