import json
import logging

from octue import definitions
from octue.exceptions import InvalidInputException, InvalidManifestException
from octue.mixins import Hashable, Identifiable, Loggable, Pathable, Serialisable
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder
from .dataset import Dataset


module_logger = logging.getLogger(__name__)


class Manifest(Pathable, Serialisable, Loggable, Identifiable, Hashable):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage."""

    _ATTRIBUTES_TO_HASH = "datasets", "keys"

    def __init__(self, id=None, logger=None, path=None, path_from=None, **kwargs):
        """Construct a Manifest"""
        super().__init__(id=id, logger=logger, path=path, path_from=path_from)

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
        for key, dataset in zip(key_list, datasets):
            if isinstance(dataset, Dataset):
                self.datasets.append(dataset)
            else:
                self.datasets.append(Dataset(**dataset, path=key, path_from=self))

        # Instantiate the rest of everything!
        self.__dict__.update(**kwargs)

    @classmethod
    def from_cloud(cls, project_name, bucket_name, directory_path):
        """Instantiate a Manifest from Google Cloud storage.

        :param str project_name:
        :param str bucket_name:
        :param str directory_path:
        :return Dataset:
        """
        storage_client = GoogleCloudStorageClient(project_name=project_name)

        serialised_manifest = json.loads(
            storage_client.download_as_string(
                bucket_name=bucket_name,
                path_in_bucket=storage.path.join(directory_path, definitions.MANIFEST_FILENAME),
            )
        )

        datasets = []

        for blob in storage_client.scandir(
            bucket_name=bucket_name,
            directory_path=directory_path,
            filter=lambda blob: (
                blob.name.endswith(definitions.DATASET_FILENAME)
                and storage.path.dirname(blob.name, name_only=True) in serialised_manifest["datasets"]
            ),
        ):
            dataset_directory_path = storage.path.split(blob.name)[0]

            datasets.append(
                Dataset.from_cloud(
                    project_name=project_name, bucket_name=bucket_name, path_to_dataset_directory=dataset_directory_path
                )
            )

        return Manifest(
            id=serialised_manifest["id"],
            path=storage.path.generate_gs_path(bucket_name, directory_path),
            hash_value=serialised_manifest["hash_value"],
            datasets=datasets,
            keys=serialised_manifest["keys"],
        )

    def to_cloud(self, project_name, bucket_name, output_directory):
        """Upload a manifest to a cloud location.

        :param str project_name:
        :param str bucket_name:
        :param str output_directory:
        :return None:
        """
        for dataset in self.datasets:
            dataset.to_cloud(project_name=project_name, bucket_name=bucket_name, output_directory=output_directory)

        GoogleCloudStorageClient(project_name=project_name).upload_from_string(
            string=self.serialise(shallow=True, to_string=True),
            bucket_name=bucket_name,
            path_in_bucket=storage.path.join(output_directory, definitions.MANIFEST_FILENAME),
        )

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

    def serialise(self, shallow=False, to_string=False):
        """Serialise to a dictionary of primitives or to a string. If `shallow` is `True`, the serialised `files`
        field only contains the absolute path of the dataset's files, rather than their entire representation.

        :param bool shallow:
        :param bool to_string:
        :return dict|str:
        """
        if not shallow:
            return super().serialise(to_string=to_string)

        serialised_manifest = super().serialise()
        serialised_manifest["datasets"] = [dataset.name for dataset in self.datasets]

        if to_string:
            return json.dumps(serialised_manifest, cls=OctueJSONEncoder, sort_keys=True, indent=4)
        return serialised_manifest

    @classmethod
    def deserialise(cls, serialised_manifest, from_string=False):
        """ Deserialise a Manifest from a dictionary. """
        if from_string:
            serialised_manifest = json.loads(serialised_manifest)

        return cls(
            id=serialised_manifest["id"],
            datasets=serialised_manifest["datasets"],
            keys=serialised_manifest["keys"],
            path=serialised_manifest["path"],
        )
