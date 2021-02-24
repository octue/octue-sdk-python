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

    return "/".join(paths)


def generate_gs_path(bucket_name, *paths):
    """Generate the Google Cloud storage path for a path in a bucket.

    :param str bucket_name:
    :param iter paths:
    :return str:
    """
    return CLOUD_STORAGE_PROTOCOL + join(bucket_name, *paths)


def get_bucket_from_path(path):
    """Get the bucket name from a path.

    :param str path:
    :return str:
    """
    bucket_name = strip_protocol_from_path(path).split("/")[0]
    return CLOUD_STORAGE_PROTOCOL + bucket_name


def strip_protocol_from_path(path):
    """Strip the `gs://` protocol from the path.

    :param str path:
    :return str:
    """
    return path.split(":")[1].strip("/")


def relpath(path, start):
    """Compute the relative path of an object in a bucket.

    :param str path:
    :param str start:
    :return str:
    """
    return strip_protocol_from_path(os.path.relpath(path, start=start))
