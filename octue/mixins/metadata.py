from abc import abstractmethod

import pkg_resources


class Metadata:
    _METADATA_ATTRIBUTES = tuple()

    def metadata(self, include_sdk_version=True):
        """Get the instance's metadata in primitive form.

        :param bool include_sdk_version: if `True`, include the `octue` version that instantiated the instance in the metadata
        :return dict:
        """
        metadata = {name: getattr(self, name) for name in self._METADATA_ATTRIBUTES}

        if include_sdk_version:
            metadata["sdk_version"] = pkg_resources.get_distribution("octue").version

        return metadata

    @abstractmethod
    def _set_metadata(self, metadata):
        """Set the instance's metadata.

        :param dict metadata:
        :return None:
        """
