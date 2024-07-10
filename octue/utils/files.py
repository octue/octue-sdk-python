import logging
import tempfile


logger = logging.getLogger(__name__)


temporary_directories = []


class RegisteredTemporaryDirectory(tempfile.TemporaryDirectory):
    """A temporary directory that's registered at instantiation so it can be referenced later."""

    def __init__(self, suffix=None, prefix=None, dir=None, ignore_cleanup_errors=False):
        super().__init__(suffix=suffix, prefix=prefix, dir=dir, ignore_cleanup_errors=ignore_cleanup_errors)
        temporary_directories.append(self)
        logger.warning("DIAGNOSTICS! Using temporary directory %r.", self.name)
