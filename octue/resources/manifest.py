import logging

from octue.exceptions import InvalidInputException
from octue.mixins import Identifiable, Loggable, Serialisable
from .dataset import Dataset


module_logger = logging.getLogger(__name__)


class Manifest(Serialisable, Loggable, Identifiable):
    """ A representation of a manifest, which can contain multiple datasets
    This is used to manage all files coming into (or leaving), a data service for an analysis at the
    configuration, input or output stage.
    """

    def __init__(self, id=None, logger=None, **kwargs):
        """ Construct a Manifest
        """
        super().__init__(id=id, logger=logger)
        self.datasets = kwargs.pop("datasets", list())
        self.keys = kwargs.pop("keys", dict())
        self.__dict__.update(**kwargs)

        # TODO we need to add keys to the manifest file schema so that we know what dataset(s) map to what keys

    def get_dataset(self, key):
        """ Gets a dataset by its key name (as defined in the twine)
        """
        idx = self.keys.get(key, None)
        if idx is None:
            raise InvalidInputException(
                f"Attempted to fetch unknown dataset '{key}' from Manifest. Allowable keys are: {list(self.keys.keys())}"
            )

        return self.datasets[idx]

    def prepare(self, data):
        """ Prepare new manifest from a manifest_spec
        """
        if len(self.datasets) > 0:
            raise InvalidInputException("You cannot `prepare()` a manifest already instantiated with datasets")

        for idx, dataset_spec in enumerate(data):

            self.keys[dataset_spec["key"]] = idx
            # TODO generate a unique name based on the filter key, tag datasets so that the tag filters in the spec
            #  apply automatically and generate a description of the dataset
            self.datasets.append(Dataset(logger=self.logger))

        return self
