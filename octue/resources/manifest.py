import logging

from octue.exceptions import InvalidInputException, InvalidManifestException
from octue.mixins import Identifiable, Loggable, Pathable, Serialisable
from .dataset import Dataset


module_logger = logging.getLogger(__name__)


class Manifest(Pathable, Serialisable, Loggable, Identifiable):
    """ A representation of a manifest, which can contain multiple datasets
    This is used to manage all files coming into (or leaving), a data service for an analysis at the
    configuration, input or output stage.
    """

    def __init__(self, id=None, logger=None, path=None, path_from=None, base_from=None, **kwargs):
        """ Construct a Manifest
        """
        super().__init__(id=id, logger=logger, path=path, path_from=path_from, base_from=base_from)

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then tidy up this hackjob. Also need to allow Pathables to update ownership
        #  (because decoders work from the bottom of the tree upwards, not top-down)

        datasets = kwargs.pop("datasets", list())
        self.keys = kwargs.pop("keys", dict())

        # TODO we need to add keys to the manifest file schema in twined so that we know what dataset(s) map to what keys
        #  In the meantime, we enforce at this level that keys will match
        n_keys = len(self.keys.keys())
        n_datasets = len(datasets)
        if n_keys != n_datasets:
            raise InvalidManifestException(
                f"Manifest instantiated with {n_keys} keys, and {n_datasets} datasets... keys must match datasets!"
            )

        # Sort the keys by the dataset index so we have a list of keys in the same order as the dataset list.
        # We'll use this to name the dataset folders
        key_list = [k for k, v in sorted(self.keys.items(), key=lambda item: item[1])]

        # Instantiate the datasets if not already done
        self.datasets = []
        for key, ds in zip(key_list, datasets):
            if isinstance(ds, Dataset):
                self.datasets.append(ds)
            else:
                self.datasets.append(Dataset(**ds, path=key, path_from=self))

        # Instantiate the rest of everything!
        self.__dict__.update(**kwargs)

    def get_dataset(self, key):
        """ Gets a dataset by its key name (as defined in the twine)
        :return: Dataset selected by its key
        :rtype: Dataset
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
            self.datasets.append(Dataset(logger=self.logger, path_from=self, path=dataset_spec["key"]))

        return self
