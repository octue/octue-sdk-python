import json
import os

import octue.migrations.manifest
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.migrations.cloud_storage import translate_bucket_name_and_path_in_bucket_to_cloud_path
from octue.mixins import Hashable, Identifiable, Pathable, Serialisable
from octue.resources.dataset import Dataset


class Manifest(Pathable, Serialisable, Identifiable, Hashable):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage.

    :param str|None id: the UUID of the manifest (a UUID is generated if one isn't given)
    :param str|None path: the path the manifest exists at (defaults to the current working directory)
    :param dict(str, octue.resources.dataset.Dataset|dict|str)|None datasets: a mapping of dataset names to `Dataset` instances, serialised datasets, or paths to datasets
    :return None:
    """

    _ATTRIBUTES_TO_HASH = ("datasets",)
    _SERIALISE_FIELDS = "datasets", "keys", "id", "name", "path"

    def __init__(self, id=None, path=None, datasets=None, **kwargs):
        if isinstance(datasets, list):
            datasets = octue.migrations.manifest.translate_datasets_list_to_dictionary(datasets, kwargs.get("keys"))

        super().__init__(id=id, path=path)
        self.datasets = {}

        # TODO The decoders aren't being used; utils.decoders.OctueJSONDecoder should be used in twined
        #  so that resources get automatically instantiated.
        #  Add a proper `decoder` argument  to the load_json utility in twined so that datasets, datafiles and manifests
        #  get initialised properly, then tidy up this hackjob. Also need to allow Pathables to update ownership
        #  (because decoders work from the bottom of the tree upwards, not top-down)

        self._instantiate_datasets(datasets or {})
        vars(self).update(**kwargs)

    @classmethod
    def from_cloud(
        cls,
        cloud_path=None,
        bucket_name=None,
        path_to_manifest_file=None,
    ):
        """Instantiate a Manifest from Google Cloud storage.

        :param str|None cloud_path: full path to manifest in cloud storage (e.g. `gs://bucket_name/path/to/manifest.json`)
        :return Dataset:
        """
        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_to_manifest_file)

        bucket_name, path_to_manifest_file = storage.path.split_bucket_name_from_gs_path(cloud_path)

        serialised_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))

        datasets = {}

        for key, dataset in serialised_manifest["datasets"].items():
            datasets[key] = Dataset.from_cloud(dataset)

        return Manifest(
            id=serialised_manifest["id"],
            path=storage.path.generate_gs_path(bucket_name, path_to_manifest_file),
            datasets=datasets,
        )

    def to_cloud(self, cloud_path=None, bucket_name=None, path_to_manifest_file=None, store_datasets=False):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory.

        :param str|None cloud_path: full path to cloud storage location to store manifest at (e.g. `gs://bucket_name/path/to/manifest.json`)
        :param bool store_datasets: if True, upload datasets to same directory as manifest file
        :return str: gs:// path for manifest file
        """
        if not cloud_path:
            cloud_path = translate_bucket_name_and_path_in_bucket_to_cloud_path(bucket_name, path_to_manifest_file)

        bucket_name, path_to_manifest_file = storage.path.split_bucket_name_from_gs_path(cloud_path)

        datasets = {}
        output_directory = storage.path.dirname(path_to_manifest_file)

        for key, dataset in self.datasets.items():
            if store_datasets:
                dataset_path = dataset.to_cloud(storage.path.generate_gs_path(bucket_name, output_directory))
                datasets[key] = dataset_path
            else:
                datasets[key] = dataset.cloud_path or dataset.absolute_path

        serialised_manifest = self.to_primitive()
        serialised_manifest["datasets"] = datasets
        del serialised_manifest["path"]

        GoogleCloudStorageClient().upload_from_string(string=json.dumps(serialised_manifest), cloud_path=cloud_path)
        return cloud_path

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        return all(dataset.all_files_are_in_cloud for dataset in self.datasets.values())

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
            self.datasets[key] = Dataset(path_from=self, path=key)

        return self

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
        :return None:
        """
        for key, dataset in datasets.items():

            if isinstance(dataset, Dataset):
                self.datasets[key] = dataset

            else:
                # If `dataset` is just a path to a dataset:
                if isinstance(dataset, str):
                    if storage.path.is_qualified_cloud_path(dataset):
                        self.datasets[key] = Dataset.from_cloud(cloud_path=dataset, recursive=True)
                    else:
                        self.datasets[key] = Dataset.from_local_directory(path_to_directory=dataset, recursive=True)

                # If `dataset` is a dictionary including a "path" key:
                elif "path" in dataset:
                    # If the path is not a cloud path or an absolute local path:
                    if not os.path.isabs(dataset["path"]) and not storage.path.is_qualified_cloud_path(dataset["path"]):
                        path = dataset.pop("path")
                        self.datasets[key] = Dataset(**dataset, path=path, path_from=self)

                    # If the path is a cloud path or an absolute local path:
                    else:
                        self.datasets[key] = Dataset(**dataset)

                else:
                    self.datasets[key] = Dataset(**dataset, path=key, path_from=self)
