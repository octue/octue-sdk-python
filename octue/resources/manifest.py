import json
import logging
import os

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException, InvalidManifestException
from octue.mixins import Hashable, Identifiable, Loggable, Pathable, Serialisable
from .dataset import Dataset


module_logger = logging.getLogger(__name__)


class Manifest(Pathable, Serialisable, Loggable, Identifiable, Hashable):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage."""

    _ATTRIBUTES_TO_HASH = "datasets", "keys"
    _SERIALISE_FIELDS = "datasets", "keys", "hash_value", "id", "name", "path"

    def __init__(self, id=None, logger=None, path=None, datasets=None, keys=None, **kwargs):
        super().__init__(id=id, logger=logger, path=path)

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then tidy up this hackjob. Also need to allow Pathables to update ownership
        #  (because decoders work from the bottom of the tree upwards, not top-down)

        datasets = datasets or []
        self.keys = keys or {}

        # TODO we need to add keys to the manifest file schema in twined so that we know what dataset(s) map to what keys
        #  In the meantime, we enforce at this level that keys will match
        if len(self.keys) != len(datasets):
            raise InvalidManifestException(
                f"Manifest instantiated with {len(self.keys)} keys, and {len(datasets)} datasets... keys must match datasets!"
            )

        # Sort the keys by the dataset index so we have a list of keys in the same order as the dataset list.
        # We'll use this to name the dataset folders
        key_list = [key for key, value in sorted(self.keys.items(), key=lambda item: item[1])]
        self._instantiate_datasets(datasets, key_list)
        vars(self).update(**kwargs)

    @classmethod
    def from_cloud(cls, project_name, bucket_name, path_to_manifest_file):
        """Instantiate a Manifest from Google Cloud storage.

        :param str project_name:
        :param str bucket_name:
        :param str path_to_manifest_file:
        :return Dataset:
        """
        storage_client = GoogleCloudStorageClient(project_name=project_name)

        serialised_manifest = json.loads(
            storage_client.download_as_string(bucket_name=bucket_name, path_in_bucket=path_to_manifest_file)
        )

        datasets = []

        for dataset in serialised_manifest["datasets"]:
            dataset_bucket_name, path = storage.path.split_bucket_name_from_gs_path(dataset)

            datasets.append(
                Dataset.from_cloud(
                    project_name=project_name, bucket_name=dataset_bucket_name, path_to_dataset_directory=path
                )
            )

        return Manifest(
            id=serialised_manifest["id"],
            path=storage.path.generate_gs_path(bucket_name, path_to_manifest_file),
            hash_value=serialised_manifest["hash_value"],
            datasets=datasets,
            keys=serialised_manifest["keys"],
        )

    def to_cloud(self, project_name, bucket_name, path_to_manifest_file, store_datasets=True):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory.

        :param str project_name:
        :param str bucket_name:
        :param str path_to_manifest_file:
        :param bool store_datasets: if True, upload datasets to same directory as manifest file
        :return str: gs:// path for manifest file
        """
        datasets = []
        output_directory = storage.path.dirname(path_to_manifest_file)

        for dataset in self.datasets:

            if store_datasets:
                dataset_path = dataset.to_cloud(project_name, bucket_name, output_directory=output_directory)
                datasets.append(dataset_path)
            else:
                datasets.append(dataset.absolute_path)

        serialised_manifest = self.serialise()
        serialised_manifest["datasets"] = sorted(datasets)
        del serialised_manifest["path"]

        GoogleCloudStorageClient(project_name=project_name).upload_from_string(
            string=json.dumps(serialised_manifest),
            bucket_name=bucket_name,
            path_in_bucket=path_to_manifest_file,
        )

        return storage.path.generate_gs_path(bucket_name, path_to_manifest_file)

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        if not self.datasets:
            return False

        return all(dataset.all_files_are_in_cloud for dataset in self.datasets)

    def get_dataset(self, key):
        """Gets a dataset by its key name (as defined in the twine)

        :return Dataset: Dataset selected by its key
        """
        idx = self.keys.get(key, None)
        if idx is None:
            raise InvalidInputException(
                f"Attempted to fetch unknown dataset '{key}' from Manifest. Allowable keys are: {list(self.keys.keys())}"
            )

        return self.datasets[idx]

    def prepare(self, data):
        """Prepare new manifest from a manifest_spec"""
        if len(self.datasets) > 0:
            raise InvalidInputException("You cannot `prepare()` a manifest already instantiated with datasets")

        for idx, dataset_spec in enumerate(data):

            self.keys[dataset_spec["key"]] = idx
            # TODO generate a unique name based on the filter key, tag datasets so that the tag filters in the spec
            #  apply automatically and generate a description of the dataset
            self.datasets.append(Dataset(logger=self.logger, path_from=self, path=dataset_spec["key"]))

        return self

    def _instantiate_datasets(self, datasets, key_list):
        """Add the given datasets to the manifest, instantiating them if needed and giving them the correct path.
        There are several possible forms the datasets can come in:
        * Instantiated Dataset instances
        * Fully serialised form - includes path
        * manifest.json form - does not include path
        * Including datafiles that already exist
        * Including datafiles that don't yet exist or are not possessed currently (e.g. future output locations or
          cloud files

        :param iter(any) datasets:
        :param list key_list:
        :return None:
        """
        self.datasets = []
        for key, dataset in zip(key_list, datasets):

            if isinstance(dataset, Dataset):
                self.datasets.append(dataset)

            else:
                if "path" in dataset:
                    if not os.path.isabs(dataset["path"]):
                        path = dataset.pop("path")
                        self.datasets.append(Dataset(**dataset, path=path, path_from=self))
                    else:
                        self.datasets.append(Dataset(**dataset))

                else:
                    self.datasets.append(Dataset(**dataset, path=key, path_from=self))
