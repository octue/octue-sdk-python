import hashlib
import logging
import os
import time

from octue.exceptions import FileNotFoundException, InvalidInputException
from octue.mixins import Identifiable, Loggable, Pathable, Serialisable, Taggable
from octue.utils import isfile


module_logger = logging.getLogger(__name__)


class Datafile(Taggable, Serialisable, Pathable, Loggable, Identifiable):
    """ Class for representing data files on the Octue system

    Files in a manifest look like this:

        {
          "path": "folder/subfolder/file_1.csv",
          "cluster": 0,
          "sequence": 0,
          "extension": "csv",
          "tags": "",
          "posix_timestamp": 0,
          "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
          "last_modified": "2019-02-28T22:40:30.533005Z",
          "size_bytes": 59684813,
          "sha-512/256": "somesha"
        },

    :parameter path_from: The root Pathable object (typically a Dataset) that this Datafile's path is relative to.
    :type path_from: Pathable

    :parameter base_from: A Pathable object, which in most circumstances is the same as the path_from object, upon which
    the `relative_path` property is based (if not given, `relative_path` is relative to current working directory

    :parameter path: The path of this file, which may include folders or subfolders, within the dataset. If no path_from
    parameter is set, then absolute paths are acceptable, otherwise relative paths are required.
    :type path: Union[str, path-like]

    :parameter logger: A logger instance to which operations with this datafile will be logged. Defaults to the module logger.
    :type logger: logging.Logger

    :parameter id: The Universally Unique ID of this file (checked to be valid if not None, generated if None)
    :type id: str

    :parameter cluster: The cluster of files, within a dataset, to which this belongs (default 0)
    :type cluster: int

    :parameter sequence: A sequence number of this file within its cluster (if sequences are appropriate)
    :type sequence: int

    :parameter tags: Space-separated string of tags relevant to this file
    :type tags: str

    :parameter posix_timestamp: A posix timestamp associated with the file, in seconds since epoch, typically when it
    was created but could relate to a relevant time point for the data
    :type posix_timestamp: number
    """

    def __init__(
        self,
        id=None,
        logger=None,
        path=None,
        path_from=None,
        base_from=None,
        cluster=0,
        sequence=None,
        tags=None,
        posix_timestamp=None,
        skip_checks=True,
        **kwargs,
    ):
        """ Construct a datafile
        """
        super().__init__(id=id, logger=logger, tags=tags, path=path, path_from=path_from, base_from=base_from)

        self.cluster = cluster

        self.sequence = sequence
        self.posix_timestamp = posix_timestamp or time.time()

        if path is None:
            raise InvalidInputException("You must supply a valid 'path' for a Datafile")

        # Set up the file extension or get it from the file path if none passed
        self.extension = self._get_extension_from_path()

        # Run integrity checks on the file
        if not skip_checks:
            self.check(**kwargs)

    def _get_extension_from_path(self, path=None):
        """ Gets extension of a file, either from a provided file path or from self.path field
        """
        path = path or self.path
        return os.path.splitext(path)[-1].strip(".")

    def _get_sha_256(self):
        """ Calculate the SHA256 hash string of the file
        """
        sha256_hash = hashlib.sha256()
        with open(self.absolute_path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()

    @property
    def name(self):
        return str(os.path.split(self.path)[-1])

    @property
    def last_modified(self):
        return os.path.getmtime(self.absolute_path)

    @property
    def size_bytes(self):
        return os.path.getsize(self.absolute_path)

    @property
    def sha_256(self):
        return self._get_sha_256()

    def check(self, size_bytes=None, sha=None, last_modified=None, extension=None):
        """ Check file presence and integrity
        """
        # TODO Check consistency of size_bytes input against self.size_bytes property for a file if we have one
        # TODO Check consistency of sha against file contents if we have a file
        # TODO Check consistency of last_modified date

        if (extension is not None) and not self.path.endswith(extension):
            raise InvalidInputException(
                f"Extension provided ({extension}) does not match file extension (from {self.path}). Pass extension=None to set extension from filename automatically."
            )

        if not self.exists():
            raise FileNotFoundException(f"No file found at {self.absolute_path}")

    def exists(self):
        """ Returns true if the datafile exists on the current system, false otherwise
        """
        return isfile(self.absolute_path)
