import concurrent.futures
import copy
import json
import logging
import threading

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.mixins import Hashable, Identifiable, Metadata, Serialisable
from octue.resources.dataset import Dataset


logger = logging.getLogger(__name__)

# A lock to ensure only one thread can use the `Manifest.update_dataset_paths` method at a time. This avoids a race
# condition where a manifest shared across multiple questions in e.g. `Child.ask_multiple` has its dataset paths
# updated by more than one thread simultaneously while attempting to convert them to signed URLs. This lock should
# perhaps be applied to all methods where mutations are made to the manifest instead of just this single method.
update_dataset_paths_lock = threading.Lock()


class Manifest(Serialisable, Identifiable, Hashable, Metadata):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage.

    :param dict(str, octue.resources.dataset.Dataset|dict|str)|None datasets: a mapping of dataset names to `Dataset` instances, serialised datasets, or paths to datasets
    :param bool ignore_stored_metadata: if `True`, ignore any metadata stored for the manifest's datasets and datafiles locally or in the cloud
    :param str|None id: the UUID of the manifest (a UUID is generated if one isn't given)
    :param str|None name: an optional name to give to the manifest
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("datasets",)
    _METADATA_ATTRIBUTES = ("id",)

    # Paths to datasets are added to the serialisation in `Manifest.to_primitive`.
    _SERIALISE_FIELDS = (*_METADATA_ATTRIBUTES, "name")

    def __init__(self, datasets=None, ignore_stored_metadata=False, id=None, name=None):
        super().__init__(id=id, name=name)
        self._ignore_stored_metadata = ignore_stored_metadata
        self.datasets = self._instantiate_datasets(datasets or {})

    @classmethod
    def from_cloud(cls, cloud_path, ignore_stored_metadata=False):
        """Instantiate a manifest from a JSON serialisation of one in Google Cloud Storage.

        :param str cloud_path: full path to manifest in cloud storage (e.g. `gs://bucket_name/path/to/manifest.json`)
        :param bool ignore_stored_metadata: if `True`, ignore any metadata stored for the manifest's datasets and datafiles in the cloud
        :return octue.resources.manifest.Manifest:
        """
        serialised_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))

        return Manifest(
            id=serialised_manifest["id"],
            datasets=serialised_manifest["datasets"],
            ignore_stored_metadata=ignore_stored_metadata,
        )

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        return all(dataset.all_files_are_in_cloud for dataset in self.datasets.values())

    def update_dataset_paths(self, path_generator):
        """Update the path of each dataset according to the given path generator function. This method is thread-safe.

        :param callable path_generator: a function taking a `Dataset` as its only argument and returning the new path of the dataset
        :return None:
        """
        with update_dataset_paths_lock:
            for name, dataset in self.datasets.items():
                self.datasets[name].path = path_generator(dataset)

    def use_signed_urls_for_datasets(self):
        """Generate signed URLs for any cloud datasets in the manifest and use these as their paths instead of regular
        cloud paths. URLs will not be generated for any local datasets or datasets whose paths are already URLs
        (including those whose paths are already signed), making this method idempotent.

        :return None:
        """

        def signed_url_path_generator(dataset):
            if dataset.exists_in_cloud and storage.path.is_cloud_uri(dataset.path):
                return dataset.generate_signed_url()
            return dataset.path

        self.update_dataset_paths(signed_url_path_generator)
        logger.debug("Cloud paths (cloud URIs) for datasets replaced with signed URLs in %r.", self)

    def to_cloud(self, cloud_path):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory.

        :param str cloud_path: full path to cloud storage location to store manifest at (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return None:
        """
        GoogleCloudStorageClient().upload_from_string(string=self.serialise(), cloud_path=cloud_path)

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
            self.datasets[key] = Dataset(path=key, ignore_stored_metadata=self._ignore_stored_metadata)

        return self

    def to_primitive(self):
        """Convert the manifest to a dictionary of primitives, converting its datasets into their paths for a
        lightweight serialisation.

        :return dict:
        """
        self_as_primitive = super().to_primitive()
        self_as_primitive["datasets"] = {}

        for name, dataset in self.datasets.items():
            if dataset._instantiated_from_files_argument:
                self_as_primitive["datasets"][name] = dataset.to_primitive()
            else:
                self_as_primitive["datasets"][name] = dataset.path

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
            return (key, Dataset(path=dataset, recursive=True, ignore_stored_metadata=self._ignore_stored_metadata))

        # If `dataset` is a cloud dataset and is represented as a dictionary including a "path" key:
        if storage.path.is_cloud_path(dataset.get("path", "")):
            return (
                key,
                Dataset(
                    path=dataset["path"],
                    files=dataset.get("files"),
                    recursive=True,
                    ignore_stored_metadata=self._ignore_stored_metadata,
                ),
            )

        return (key, Dataset(**dataset, ignore_stored_metadata=self._ignore_stored_metadata))

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
