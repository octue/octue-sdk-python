import json
import os

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.exceptions import InvalidInputException
from octue.mixins import Hashable, Identifiable, Pathable, Serialisable
from octue.resources.datafile import CLOUD_STORAGE_PROTOCOL

from .dataset import Dataset


class Manifest(Pathable, Serialisable, Identifiable, Hashable):
    """A representation of a manifest, which can contain multiple datasets This is used to manage all files coming into
    (or leaving), a data service for an analysis at the configuration, input or output stage.
    """

    _ATTRIBUTES_TO_HASH = ("datasets",)
    _SERIALISE_FIELDS = "datasets", "keys", "id", "name", "path"

    def __init__(self, id=None, path=None, datasets=None, **kwargs):
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
    def from_cloud(cls, project_name, cloud_path=None, bucket_name=None, path_to_manifest_file=None):
        """Instantiate a Manifest from Google Cloud storage. Either (`bucket_name` and `path_to_manifest_file`) or
        `cloud_path` must be provided.

        :param str project_name: name of Google Cloud project manifest is stored in
        :param str|None cloud_path: full path to manifest in cloud storage (e.g. `gs://bucket_name/path/to/manifest.json`)
        :param str|None bucket_name: name of bucket manifest is stored in
        :param str|None path_to_manifest_file: path to manifest in cloud storage e.g. `path/to/manifest.json`
        :return Dataset:
        """
        if cloud_path:
            bucket_name, path_to_manifest_file = storage.path.split_bucket_name_from_gs_path(cloud_path)

        serialised_manifest = json.loads(
            GoogleCloudStorageClient(project_name=project_name).download_as_string(
                bucket_name=bucket_name, path_in_bucket=path_to_manifest_file
            )
        )

        datasets = []

        for key, dataset in serialised_manifest["datasets"].items():
            dataset_bucket_name, path = storage.path.split_bucket_name_from_gs_path(dataset)

            datasets[key] = Dataset.from_cloud(
                project_name=project_name, bucket_name=dataset_bucket_name, path_to_dataset_directory=path
            )

        return Manifest(
            id=serialised_manifest["id"],
            path=storage.path.generate_gs_path(bucket_name, path_to_manifest_file),
            datasets=datasets,
        )

    def to_cloud(
        self, project_name, cloud_path=None, bucket_name=None, path_to_manifest_file=None, store_datasets=True
    ):
        """Upload a manifest to a cloud location, optionally uploading its datasets into the same directory. Either
        (`bucket_name` and `path_to_manifest_file`) or `cloud_path` must be provided.

        :param str project_name: name of Google Cloud project to store manifest in
        :param str|None cloud_path: full path to cloud storage location to store manifest at (e.g. `gs://bucket_name/path/to/manifest.json`)
        :param str|None bucket_name: name of bucket to store manifest in
        :param str|None path_to_manifest_file: cloud storage path to store manifest at e.g. `path/to/manifest.json`
        :param bool store_datasets: if True, upload datasets to same directory as manifest file
        :return str: gs:// path for manifest file
        """
        if cloud_path:
            bucket_name, path_to_manifest_file = storage.path.split_bucket_name_from_gs_path(cloud_path)

        datasets = {}
        output_directory = storage.path.dirname(path_to_manifest_file)

        for key, dataset in self.datasets.items():

            if store_datasets:
                dataset_path = dataset.to_cloud(
                    project_name, bucket_name=bucket_name, output_directory=output_directory
                )

                datasets[key] = dataset_path

            else:
                datasets[key] = dataset.absolute_path

        serialised_manifest = self.to_primitive()
        serialised_manifest["datasets"] = sorted(datasets)
        del serialised_manifest["path"]

        GoogleCloudStorageClient(project_name=project_name).upload_from_string(
            string=json.dumps(serialised_manifest),
            bucket_name=bucket_name,
            path_in_bucket=path_to_manifest_file,
        )

        return cloud_path or storage.path.generate_gs_path(bucket_name, path_to_manifest_file)

    @property
    def all_datasets_are_in_cloud(self):
        """Do all the files of all the datasets of the manifest exist in the cloud?

        :return bool:
        """
        if not self.datasets:
            return False

        return all(dataset.all_files_are_in_cloud for dataset in self.datasets.values())

    def get_dataset(self, key):
        """Get a dataset by its key name (as defined in the twine).

        :return Dataset: Dataset selected by its key
        """
        dataset = self.datasets.get(key, None)

        if dataset is None:
            raise InvalidInputException(
                f"Attempted to fetch unknown dataset {key!r} from Manifest. Allowable keys are: {self.datasets.keys()}"
            )

        return self.datasets[dataset]

    def prepare(self, data):
        """Prepare new manifest from a manifest_spec"""
        if len(self.datasets) > 0:
            raise InvalidInputException("You cannot `prepare()` a manifest already instantiated with datasets")

        for index, dataset_specification in enumerate(data["datasets"]):
            # TODO generate a unique name based on the filter key, label datasets so that the label filters in the spec
            #  apply automatically and generate a description of the dataset
            self.datasets[dataset_specification["key"]] = Dataset(path_from=self, path=dataset_specification["key"])

        return self

    def _instantiate_datasets(self, datasets):
        """Add the given datasets to the manifest, instantiating them if needed and giving them the correct path.
        There are several possible forms the datasets can come in:
        * Instantiated Dataset instances
        * Fully serialised form - includes path
        * manifest.json form - does not include path
        * Including datafiles that already exist
        * Including datafiles that don't yet exist or are not possessed currently (e.g. future output locations or
          cloud files

        :param iter(any) datasets:
        :return None:
        """
        for key, dataset in datasets.items():

            if isinstance(dataset, Dataset):
                self.datasets[key] = dataset

            else:
                if "path" in dataset:
                    if not os.path.isabs(dataset["path"]) and not dataset["path"].startswith(CLOUD_STORAGE_PROTOCOL):
                        path = dataset.pop("path")
                        self.datasets[key] = Dataset(**dataset, path=path, path_from=self)
                    else:
                        self.datasets[key] = Dataset(**dataset)

                else:
                    self.datasets[key] = Dataset(**dataset, path=key, path_from=self)
