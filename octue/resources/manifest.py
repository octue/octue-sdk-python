import copy
import json
import os
import warnings

import octue.migrations.manifest
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.migrations.cloud_storage import translate_bucket_name_and_path_in_bucket_to_cloud_path
from octue.mixins import Hashable, Identifiable, Serialisable
from octue.resources.dataset import Dataset


class Manifest(Serialisable, Identifiable, Hashable):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage.

    :param str|None id: the UUID of the manifest (a UUID is generated if one isn't given)
    :param str|None path: the path the manifest exists at (defaults to the current working directory)
    :param dict(str, octue.resources.dataset.Dataset|dict|str)|None datasets: a mapping of dataset names to `Dataset` instances, serialised datasets, or paths to datasets
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("datasets",)

    # Paths to datasets are added to the serialisation in `Manifest.to_primitive`.
    _SERIALISE_FIELDS = "id", "name"

    def __init__(self, id=None, name=None, datasets=None, **kwargs):
        if isinstance(datasets, list):
            datasets = octue.migrations.manifest.translate_datasets_list_to_dictionary(datasets, kwargs.get("keys"))

        super().__init__(id=id, name=name)
        self.datasets = self._instantiate_datasets(datasets or {})

    @classmethod
    def from_cloud(cls, cloud_path=None, bucket_name=None, path_to_manifest_file=None):
        """Instantiate a Manifest from Google Cloud storage.

        :param str|None cloud_path: full path to manifest in cloud storage (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return Dataset:
        """
        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_to_manifest_file)

        serialised_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))

        return Manifest(
            id=serialised_manifest["id"],
            datasets={key: Dataset.from_cloud(dataset) for key, dataset in serialised_manifest["datasets"].items()},
        )

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        return all(dataset.all_files_are_in_cloud for dataset in self.datasets.values())

    def to_cloud(self, cloud_path=None, bucket_name=None, path_to_manifest_file=None, store_datasets=None):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory.

        :param str|None cloud_path: full path to cloud storage location to store manifest at (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return None:
        """
        if store_datasets:
            warnings.warn(
                message=(
                    "The `store_datasets` parameter is no longer available - please call the `to_cloud` method on any "
                    "datasets you wish to upload to the cloud separately."
                ),
                category=DeprecationWarning,
            )

        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_to_manifest_file)

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
        There are several possible forms the datasets can come in:
        * Instantiated Dataset instances
        * A list of dictionaries pointing to cloud datasets
        * Fully serialised form - includes path
        * `manifest.json` form - does not include path
        * Including datafiles that already exist
        * Including datafiles that don't yet exist or are not possessed currently (e.g. future output locations or
          cloud files)

        :param dict(str, octue.resources.dataset.Dataset|dict|str) datasets: the datasets to add to the manifest
        :return dict:
        """
        datasets = copy.deepcopy(datasets)
        datasets_to_add = {}

        for key, dataset in datasets.items():

            if isinstance(dataset, Dataset):
                datasets_to_add[key] = dataset

            else:
                # If `dataset` is just a path to a dataset:
                if isinstance(dataset, str):
                    if storage.path.is_qualified_cloud_path(dataset):
                        datasets_to_add[key] = Dataset.from_cloud(cloud_path=dataset, recursive=True)
                    else:
                        datasets_to_add[key] = Dataset.from_local_directory(path_to_directory=dataset, recursive=True)

                # If `dataset` is a dictionary including a "path" key:
                elif "path" in dataset:

                    # If the path is a cloud path:
                    if storage.path.is_qualified_cloud_path(dataset["path"]):
                        datasets_to_add[key] = Dataset.from_cloud(cloud_path=dataset["path"], recursive=True)

                    # If the path is local but not absolute:
                    elif not os.path.isabs(dataset["path"]):
                        path = dataset.pop("path")
                        datasets_to_add[key] = Dataset(**dataset, path=path)

                    # If the path is an absolute local path:
                    else:
                        datasets_to_add[key] = Dataset(**dataset)

                else:
                    datasets_to_add[key] = Dataset(**dataset, path=key)

        return datasets_to_add
