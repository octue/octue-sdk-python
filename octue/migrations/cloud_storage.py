import warnings

from octue.cloud import storage


def translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_in_bucket):
    warnings.warn(
        message=(
            "Using a bucket name and path in bucket will be deprecated soon. Please use `cloud_path` instead e.g."
            "'gs://bucket_name/path/to/file.txt'."
        ),
        category=DeprecationWarning,
    )

    return storage.path.generate_gs_path(bucket_name, path_in_bucket)
