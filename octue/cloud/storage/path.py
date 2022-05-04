import os
from urllib.parse import urlparse


CLOUD_STORAGE_PROTOCOL = "gs://"


def is_cloud_path(path):
    """Determine if the given path is either a cloud storage URI or a URL.

    :param str path: the path to check
    :return bool:
    """
    return is_cloud_uri(path) or is_url(path)


def is_cloud_uri(path):
    """Determine if the given path is a cloud storage URI - i.e. if it begins with the cloud storage protocol.

    :param str path: the path to check
    :return bool: `True` if the path starts with the cloud storage protocol
    """
    return path.startswith(CLOUD_STORAGE_PROTOCOL)


def is_url(path):
    """Determine if the given path is a URL.

    :param str path: the path to check
    :return bool: `True` if the path starts with "http"
    """
    return path.startswith("http")


def join(*paths):
    """Join segments of path into a valid Google Cloud storage path. This is an analogue to `os.path.join` for Google
    Cloud storage paths.

    :param iter paths:
    :return str:
    """
    if not paths:
        return ""

    path = os.path.normpath(os.path.join(*paths)).replace("\\", "/")

    if path.startswith("gs:/"):
        if not is_cloud_uri(path):
            path = path.replace("gs:/", CLOUD_STORAGE_PROTOCOL)

    return path


def generate_gs_path(bucket_name, *paths):
    """Generate the Google Cloud Storage URI for a path in a bucket.

    :param str bucket_name:
    :param iter paths:
    :return str:
    """
    if not paths:
        return CLOUD_STORAGE_PROTOCOL + bucket_name
    return CLOUD_STORAGE_PROTOCOL + join(bucket_name, paths[0].lstrip("/"), *paths[1:])


def split_bucket_name_from_cloud_path(path):
    """Split the bucket name from the path within the bucket and return both.

    :param str path: the path to split
    :return (str, str): the bucket name and the path within the bucket
    """
    if is_cloud_uri(path):
        path = strip_protocol_from_path(path).split("/")
        return path[0], join(*path[1:])

    path = urlparse(path).path.split("/")
    return path[1], join(*path[2:])


def strip_protocol_from_path(path):
    """Strip the `gs://` protocol from the path.

    :param str path:
    :return str:
    """
    if not is_cloud_path(path):
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

    return os.path.relpath(strip_protocol_from_path(path), start).replace("\\", "/")


def split(path):
    """Split a path into its head and tail. `storage.path.split` (this function) is the analogue of `os.path.split` for
    Google Cloud Storage paths.

    :param str path:
    :return (str, str):
    """
    paths = path.split("/")
    return join(*paths[:-1]), paths[-1]


def dirname(path, name_only=False):
    """Get the path of the directory of the given path. If `name_only` is `True`, just get the name of the directory.

    :param str path:
    :param bool name_only:
    :return str:
    """
    directory_path = os.path.dirname(path).replace("\\", "/")

    if name_only:
        return split(directory_path)[-1]

    return directory_path
