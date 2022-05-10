from urllib.parse import urlparse

from octue.cloud import storage
from octue.exceptions import CloudLocationNotSpecified


class CloudPathable:
    _CLOUD_PATH_ATTRIBUTE_NAME = "path"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def __cloud_path(self):
        return getattr(self, self._CLOUD_PATH_ATTRIBUTE_NAME)

    @property
    def exists_in_cloud(self):
        """Return `True` if the dataset exists in the cloud.

        :return bool:
        """
        if self.__cloud_path:
            return storage.path.is_cloud_path(self.__cloud_path)
        return None

    @property
    def exists_locally(self):
        """Return `True` if the dataset exists locally.

        :return bool:
        """
        return not self.exists_in_cloud

    @property
    def cloud_protocol(self):
        """Get the cloud protocol of the instance if it exists in the cloud (e.g. "gs" for a cloud path of
        "gs://my-bucket/my-file.txt").

        :return str|None:
        """
        if not self.exists_in_cloud:
            return None
        return urlparse(self.__cloud_path).scheme

    @property
    def bucket_name(self):
        """Get the name of the bucket the dataset exists in if it exists in the cloud.

        :return str|None:
        """
        if self.exists_in_cloud:
            return storage.path.split_bucket_name_from_cloud_path(self.__cloud_path)[0]
        return None

    @property
    def path_in_bucket(self):
        """Get the path of the dataset in its bucket if it exists in the cloud.

        :return str|None:
        """
        if self.exists_in_cloud:
            return storage.path.split_bucket_name_from_cloud_path(self.__cloud_path)[1]
        return None

    def _get_cloud_location(self, cloud_path=None):
        """Get the cloud location details for the instance.

        :param str|None cloud_path:
        :raise octue.exceptions.CloudLocationNotSpecified: if an exact cloud location isn't provided and isn't available implicitly (i.e. the instance wasn't loaded from the cloud previously)
        :return (str, str): project name and cloud path
        """
        cloud_path = cloud_path or self.__cloud_path

        if not cloud_path:
            self._raise_cloud_location_error()

        self._cloud_path = cloud_path
        return cloud_path

    def _raise_cloud_location_error(self):
        """Raise an error indicating that the cloud location of the instance has not yet been specified.

        :raise CloudLocationNotSpecified:
        :return None:
        """
        raise CloudLocationNotSpecified(
            f"{self!r} wasn't previously loaded from the cloud so doesn't have an implicit cloud location - please "
            f"specify its exact location (its project name and cloud path)."
        )
