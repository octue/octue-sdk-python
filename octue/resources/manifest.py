import concurrent.futures
import copy
import json
import logging

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.mixins import Hashable, Identifiable, Metadata, Serialisable
from octue.resources.dataset import Dataset


logger = logging.getLogger(__name__)


class Manifest(Serialisable, Identifiable, Hashable, Metadata):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage.

    :param dict(str, octue.resources.dataset.Dataset|dict|str)|None datasets: a mapping of dataset names to `Dataset` instances, serialised datasets, or paths to datasets
    :param str|None id: the UUID of the manifest (a UUID is generated if one isn't given)
    :param str|None name: an optional name to give to the manifest
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("datasets",)
    _METADATA_ATTRIBUTES = ("id",)

    # Paths to datasets are added to the serialisation in `Manifest.to_primitive`.
    _SERIALISE_FIELDS = (*_METADATA_ATTRIBUTES, "name")

    def __init__(self, datasets=None, id=None, name=None):
        super().__init__(id=id, name=name)
        self.datasets = self._instantiate_datasets(datasets or {})

    @classmethod
    def from_cloud(cls, cloud_path):
        """Instantiate a Manifest from Google Cloud storage.

        :param str cloud_path: full path to manifest in cloud storage (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return Dataset:
        """
        serialised_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))

        return Manifest(
            id=serialised_manifest["id"],
            datasets={key: Dataset(path=dataset) for key, dataset in serialised_manifest["datasets"].items()},
        )

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        return all(dataset.all_files_are_in_cloud for dataset in self.datasets.values())

    def use_signed_urls_for_datasets(self):
        """Generate signed URLs for any cloud datasets in the manifest and use these as their paths instead of regular
        cloud paths. URLs will not be generated for any local datasets in the manifest.

        :return None:
        """
        for name, dataset in self.datasets.items():
            if dataset.exists_in_cloud:
                self.datasets[name].path = dataset.generate_signed_url()

        logger.debug("Cloud paths (cloud URIs) for datasets replaced with signed URLs in %r.", self)

    def to_cloud(self, cloud_path):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory.

        :param str cloud_path: full path to cloud storage location to store manifest at (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return None:
        """
        GoogleCloudStorageClient().upload_from_string(string=json.dumps(self.to_primitive()), cloud_path=cloud_path)

    def get_dataset(self, key):
        """Get a dataset by its key (as defined in the twine).

        :param str key:
        :return octue.resources.dataset.Dataset:
        """
        dataset = self.datasets.get(key, None)

        if dataset is None:
            raise InvalidInputException(
                f"Attempted to fetch unknown dataset {key!r} from Manifest. Allowable keys are: "
                f"{list(self.datasets.keys())}"
            )

        return dataset

    def prepare(self, data):
        """Prepare new manifest from a manifest_spec.

        :param dict data:
        :return Manifest:
        """
        if len(self.datasets) > 0:
            raise InvalidInputException("You cannot `prepare()` a manifest already instantiated with datasets")

        for key, dataset_specification in data["datasets"].items():
            # TODO generate a unique name based on the filter key, label datasets so that the label filters in the spec
            #  apply automatically and generate a description of the dataset
            self.datasets[key] = Dataset(path=key)

        return self

    def to_primitive(self):
        """Convert the manifest to a dictionary of primitives, converting its datasets into their paths for a
        lightweight serialisation.

        :return dict:
        """
        self_as_primitive = super().to_primitive()
        self_as_primitive["datasets"] = {name: dataset.path for name, dataset in self.datasets.items()}
        return self_as_primitive

    def _instantiate_datasets(self, datasets):
        """Add the given datasets to the manifest, instantiating them if needed and giving them the correct path.
        There are several possible forms each dataset can come in:
        * Instantiated Dataset instance
        * A path to a dataset
        * Serialised form (a dictionary including a path key)

        The datasets can:
        * Including datafiles that already exist
        * Including datafiles that don't yet exist or are not possessed currently (e.g. future output locations or
          cloud files)

        :param dict(str, octue.resources.dataset.Dataset|dict|str) datasets: the datasets to add to the manifest
        :return dict:
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return dict(executor.map(self._instantiate_dataset, copy.deepcopy(datasets).items()))

    def _instantiate_dataset(self, key_and_dataset):
        """Instantiate a dataset from multiple input formats.

        :param tuple(str, any) key_and_dataset:
        :return tuple(str, octue.resources.dataset.Dataset):
        """
        key, dataset = key_and_dataset

        if isinstance(dataset, Dataset):
            return (key, dataset)

        # If `dataset` is just a path to a dataset:
        if isinstance(dataset, str):
            return (key, Dataset(path=dataset, recursive=True))

        # If `dataset` is a dictionary including a "path" key:
        if storage.path.is_cloud_path(dataset["path"]):
            return (key, Dataset(path=dataset["path"], recursive=True))

        return (key, Dataset(**dataset))

    def _set_metadata(self, metadata):
        """Set the manifest's metadata.

        :param dict metadata:
        :return None:
        """
        for attribute in self._METADATA_ATTRIBUTES:
            if attribute not in metadata:
                continue

            if attribute == "id":
                self._set_id(metadata["id"])
                continue

            setattr(self, attribute, metadata[attribute])
