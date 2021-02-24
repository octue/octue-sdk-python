CLOUD_STORAGE_PROTOCOL = "gs://"


def join(*paths):
    """An analogue to os.path.join for Google Cloud storage paths.

    :param iter paths:
    :return str:
    """
    return "/".join(paths)


def generate_gs_path(bucket_name, *paths):
    """Generate the Google Cloud storage path for a path in a bucket.

    :param str bucket_name:
    :param iter paths:
    :return str:
    """
    return CLOUD_STORAGE_PROTOCOL + join(bucket_name, *paths)
