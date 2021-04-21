import os

from octue.cloud import storage
from octue.exceptions import InvalidInputException


class Pathable:
    """Mixin class to enable resources to get their path location from an owner.

    For example, datasets can get their path from the Manifest they belong to.

    :param str|None path:
    :param Pathable|None path_from:
    :return None:
    """

    def __init__(self, *args, path=None, path_from=None, **kwargs):
        super().__init__(*args, **kwargs)

        if (path_from is not None) and not isinstance(path_from, Pathable):
            raise InvalidInputException(
                "paths_from argument must be an instance of an object inheriting from Pathable() mixin"
            )

        self._path_from = path_from
        self._path_is_absolute = False

        if path and path.startswith(storage.path.CLOUD_STORAGE_PROTOCOL):
            self._path_is_in_google_cloud_storage = True

        else:
            self._path_is_in_google_cloud_storage = False

        self.path = path

    @property
    def _path_prefix(self):
        """Gets the path prefix (this is the absolute_path of the owner path_from object).

        Defaults to the current working directory.

        :return str:
        """
        if self._path_from is not None:
            return self._path_from.absolute_path

        if self._path_is_absolute:
            return ""

        return os.getcwd()

    @property
    def absolute_path(self):
        """The absolute path of this resource.

        :return str:
        """
        if self._path_is_in_google_cloud_storage:
            return storage.path.join(self._path_prefix, self._path)

        return os.path.normpath(os.path.join(self._path_prefix, self._path))

    def path_relative_to(self, path=None):
        """Get the path of this resource relative to another. If no path is provided, the current directory is used if
        the path is for a filesystem, or the bucket is used if the path is for Google Cloud.

        :return str:
        """
        if isinstance(path, Pathable):
            path = path.absolute_path

        if self._path_is_in_google_cloud_storage:
            return storage.path.relpath(self.absolute_path, start=path)

        return os.path.relpath(self.absolute_path, start=path or os.getcwd())

    @property
    def path(self):
        """The path of this resource.

        :return str:
        """
        return self._path

    @path.setter
    def path(self, value):
        """Set the path of this resource.

        :param Union[str, path-like] value: Path of the resource. If the resource was instantiated with a `path_from`
        object, this path must be relative. Otherwise, absolute paths are acceptable.
        :return None:
        """
        if value and value.startswith(storage.path.CLOUD_STORAGE_PROTOCOL):
            path_is_absolute = True

        else:
            value = os.path.normpath(value or ".")
            path_is_absolute = os.path.isabs(value)

        if path_is_absolute and self._path_from is not None:
            raise InvalidInputException(
                f"You cannot set an absolute path on a pathable instantiated with 'path_from'. Set a path relative to "
                f"the path_from object ({self._path_from})"
            )

        self._path_is_absolute = path_is_absolute
        self._path = value
