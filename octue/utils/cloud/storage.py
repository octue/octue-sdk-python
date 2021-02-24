import os


CLOUD_STORAGE_PROTOCOL = "gs://"


def join(*paths):
    """An analogue to os.path.join for Google Cloud storage paths.

    :param iter paths:
    :return str:
    """
    paths = list(paths)

    while "" in paths:
        paths.remove("")

    if any(path.startswith("/") for path in paths[1:]):
        reverse_paths = list(reversed(paths))

        for i, path in enumerate(reverse_paths):
            if path.startswith("/"):
                return "/".join(reversed(reverse_paths[: i + 1]))

    return "/".join(paths)


def generate_gs_path(bucket_name, *paths):
    """Generate the Google Cloud storage path for a path in a bucket.

    :param str bucket_name:
    :param iter paths:
    :return str:
    """
    return CLOUD_STORAGE_PROTOCOL + join(bucket_name, *paths)


def strip_protocol_from_path(path):
    """Strip the `gs://` protocol from the path.

    :param str path:
    :return str:
    """
    if not path.startswith(CLOUD_STORAGE_PROTOCOL):
        return path
    return path.split(":")[1].lstrip("/")


def relpath(path, start):
    """Compute the relative path of an object in a bucket.

    :param str path:
    :param str start:
    :return str:
    """
    if start is not None:
        start = strip_protocol_from_path(start)

    return os.path.relpath(strip_protocol_from_path(path), start)
