import hashlib
import logging
import os
import time

from octue.exceptions import FileNotFoundException, InvalidInputException
from octue.mixins import Identifiable, Loggable, Serialisable, Taggable
from octue.utils import isfile


module_logger = logging.getLogger(__name__)


class Datafile(Taggable, Serialisable, Loggable, Identifiable):
    """ Class for representing data files on the Octue system

    Files in a manifest look like this:

        {
          "path": "input/datasets/7ead7669/file_1.csv",
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

    :parameter local_path_prefix: A path, specific to the present local system, to the directory containing the dataset
    in which this file resides. Default is the current working directory.
    :type local_path_prefix: str

    :parameter path: The path of this file, relative to the local_path_prefix (which may have a folder structure within it)
    :type path: str

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
        local_path_prefix=".",
        path=None,
        cluster=0,
        sequence=None,
        tags=None,
        posix_timestamp=None,
        skip_checks=True,
        **kwargs,
    ):
        """ Construct a datafile
        """
        super().__init__(id=id, logger=logger, tags=tags)

        self.cluster = cluster

        self.sequence = sequence
        self.posix_timestamp = posix_timestamp or time.time()

        if path is None:
            raise InvalidInputException("You must supply a valid 'path' argument")
        else:
            path = str(path).lstrip("\\/")

        # Strip to ensure path is always expressed as relative
        self.path = path

        # Replace current directory specifier in the prefix with an absolute path
        self.local_path_prefix = str(os.path.abspath(local_path_prefix))

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
        with open(self.full_path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()

    @property
    def name(self):
        return str(os.path.split(self.path)[-1])

    @property
    def full_path(self):
        return os.path.join(self.local_path_prefix, self.path)

    @property
    def last_modified(self):
        return os.path.getmtime(self.full_path)

    @property
    def size_bytes(self):
        return os.path.getsize(self.full_path)

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
            raise FileNotFoundException(f"No file found at {self.full_path}")

    def exists(self):
        """ Returns true if the datafile exists on the current system, false otherwise
        """
        return isfile(self.full_path)
