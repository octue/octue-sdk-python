import os


def isfolder(*args, make_if_absent=False):
    """Return `True` if input path is the name of a valid, existing, file. Joins multiple inputs.

    tf = isfolder(str) Returns true is str is a full (or relative from the current working directory) folder path,
    false otherwise.

    tf = isfolder(str1, str2, ...) Concatenates any number of strings using the platform-specific file separator before
    testing for presence of the folder. Equivalent to typing octue.utils.isfolder(os.path.join(str1, str2, ...))
    """
    path = os.path.join(*args)
    if make_if_absent:
        os.makedirs(path, exist_ok=True)

    return os.path.isdir(path)
