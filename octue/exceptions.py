

class InvalidInput(Exception):
    """Raise when an object is instantiated or a function called with invalid inputs
    """


class FolderNotPresent(InvalidInput):
    """ Raise when a required folder (e.g. <data_dir>/input) cannot be found
    """


class InvalidManifest(InvalidInput):
    """Raise when a manifest loaded from JSON does not pass validation
    """


class InvalidManifestType(InvalidManifest):
    """Raised when user attempts to create a manifest of a type other than 'input', 'output' or 'build'
    """


class InvalidOctueFileType(Exception):
    """Raised when you attempt to register a file type in the results manifest that Octue doesn't know about
    """


class NotImplementedYet(Exception):
    """Raised when you attempt to use a function whose high-level API is in place, but which is not implemented yet
    """


class UnexpectedNumberOfResults(Exception):
    """ Raise when searching for a single data file (or a particular number of data files) and the number of results exceeds that expected
    """

