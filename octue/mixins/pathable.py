import os

from octue.exceptions import InvalidInputException


class Pathable:
    """ Mixin class to enable resources to get their path location from an owner.

    For example, datasets can get their path from the Manifest they belong to.

    """

    def __init__(self, *args, path=None, path_from=None, base_from=None, **kwargs):
        """ Constructor for pathable mixin
        """
        super().__init__(*args, **kwargs)

        if (path_from is not None) and not isinstance(path_from, Pathable):
            raise InvalidInputException(
                "paths_from argument must be an instance of an object inheriting from Pathable() mixin"
            )

        if (base_from is not None) and not isinstance(base_from, Pathable):
            raise InvalidInputException(
                "base_from argument must be an instance of an object inheriting from Pathable() mixin"
            )

        self._path_from = path_from
        self._base_from = base_from
        self._path_is_absolute = False
        self.path = path

    @property
    def _base_path(self):
        """ Gets the absolute path of the base_from object, from which any relative paths are constructed
        :return:
        :rtype:
        """
        if self._base_from:
            return self._base_from.absolute_path

        return os.getcwd()

    @property
    def _path_prefix(self):
        """ Gets the path prefix (this is the absolute_path of the owner path_from object).
        Defaults to the current working directory
        """
        if self._path_from:
            return self._path_from.absolute_path

        if self._path_is_absolute:
            return ""

        return os.getcwd()

    @property
    def absolute_path(self):
        """ The absolute path of this resource
        """
        return os.path.normpath(os.path.join(self._path_prefix, self._path))

    @property
    def relative_path(self):
        """ The path of this resource relative to its base path
        """
        return os.path.relpath(self.absolute_path, self._base_path)

    @property
    def path(self):
        """ The path of this resource
        """
        return self._path

    @path.setter
    def path(self, value):
        """ Set the path of this resource.

        :param value: Path of the resource. If the resource was instantiated with a `path_from` object, this path must
        be relative. Otherwise, absolute paths are acceptable.
        :type value: Union[str, path-like]
        """

        value = os.path.normpath(value or ".")

        path_is_absolute = value == os.path.abspath(value)

        if path_is_absolute and self._path_from is not None:
            raise InvalidInputException(
                f"You cannot an absolute path on a pathable instantiated with 'path_from'. Set a path relative to the path_from object ({self._path_from})"
            )

        self._path_is_absolute = path_is_absolute
        self._path = value
