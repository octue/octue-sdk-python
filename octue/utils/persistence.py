import os


def calculate_disk_usage(path, filter=None):
    """Calculate the the disk usage in bytes of the file or directory at the given path. The disk usage is calculated
    recursively (i.e. if a directory is given, it includes the usage of all the files and subdirectories and so on of
    the directory). The files considered can be filtered by a callable that returns True for paths that should be
    considered and False for those that shouldn't.

    :param str path:
    :param callable|None filter:
    :return float:
    """
    if os.path.isfile(path):
        if filter is None:
            return os.path.getsize(path)

        if filter(path):
            return os.path.getsize(path)

        return 0

    return sum(calculate_disk_usage(item.path) for item in os.scandir(path))


def get_oldest_file_in_directory(path, filter=None):
    """Get the oldest file in a directory. This is not a recursive function. The files considered can be filtered by a
    callable that returns True for paths that should be considered and False for those that shouldn't.

    :param str path:
    :param callable|None filter:
    :return str|None:
    """
    if filter is None:
        contents = [item for item in os.scandir(path) if item.is_file()]
    else:
        contents = [item for item in os.scandir(path) if item.is_file() and filter(item.path)]

    try:
        return min(contents, key=os.path.getmtime).path
    except ValueError:
        return None
