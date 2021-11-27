import os


def isfile(*args):
    """Return `True` if input path is the name of a valid, existing, file.

    Joins multiple inputs, making it slightly more useful than os.path.isfile()

    tf = isfile(str) Returns true is str is a full (or relative from the current working directory) path and name of an
    existing file, false otherwise.

    tf = isfile(str1, str2, ...) Concatenates any number of strings using the platform-specific file separator before
    testing for presence of the file. Equivalent to typing octue.utils.isfile(os.path.join(str1, str2, ...))
    """
    return os.path.isfile(os.path.join(*args))
