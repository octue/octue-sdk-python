from abc import abstractmethod

import pkg_resources

from octue.mixins.hashable import Hashable


class Metadata:
    _METADATA_ATTRIBUTES = tuple()

    @property
    def metadata_hash_value(self, **kwargs):
        """Get the hash of the instance's metadata, not including its ID.

        :return str:
        """
        return Hashable.hash_non_class_object(self.metadata(include_id=False, include_sdk_version=False, **kwargs))

    def metadata(self, include_id=True, include_sdk_version=True, **kwargs):
        """Get the instance's metadata in primitive form. The metadata is the set of attributes included in the class
        variable `self._METADATA_ATTRIBUTES`.

        :param bool include_id: if `True`, include the ID of the instance if it is included in `self._METADATA_ATTRIBUTES`
        :param bool include_sdk_version: if `True`, include the `octue` version that instantiated the instance
        :return dict:
        """
        metadata = {name: getattr(self, name) for name in self._METADATA_ATTRIBUTES}

        if include_sdk_version:
            metadata["sdk_version"] = pkg_resources.get_distribution("octue").version

        if not include_id and "id" in metadata:
            del metadata["id"]

        return metadata

    @abstractmethod
    def _set_metadata(self, metadata):
        """Set the instance's metadata.

        :param dict metadata:
        :return None:
        """
