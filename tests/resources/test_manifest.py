import copy
import json
import os
import tempfile
from unittest.mock import patch

from octue.cloud import storage
from octue.cloud.emulators.cloud_storage import mock_generate_signed_url
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Datafile, Dataset, Manifest
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase
from tests.resources import create_dataset_with_two_files


class TestManifest(BaseTestCase):
    def test_hash_value(self):
        """Test hashing a manifest with multiple datasets gives a hash of length 8."""
        manifest = self.create_valid_manifest()
        hash_ = manifest.hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 8)

    def test_hashes_for_the_same_manifest_are_the_same(self):
        """Ensure the hashes for two manifests that are exactly the same are the same."""
        first_manifest = self.create_valid_manifest()
        second_manifest = copy.deepcopy(first_manifest)
        self.assertEqual(first_manifest.hash_value, second_manifest.hash_value)

    def test_all_datasets_are_in_cloud(self):
        """Test whether all files of all datasets in a manifest are in the cloud or not can be determined."""
        self.assertFalse(self.create_valid_manifest().all_datasets_are_in_cloud)
        self.assertTrue(Manifest().all_datasets_are_in_cloud)

        files = [Datafile(path="gs://hello/file.txt"), Datafile(path="gs://goodbye/file.csv")]
        manifest = Manifest(datasets={"my_dataset": Dataset(files=files)})
        self.assertTrue(manifest.all_datasets_are_in_cloud)

    def test_serialisation_and_deserialisation(self):
        """Test that manifests can be serialised and deserialised."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            dataset_0_path = os.path.join(temporary_directory, "my_dataset_0")
            os.mkdir(dataset_0_path)

            with open(os.path.join(dataset_0_path, "my_file_0.txt"), "w") as f:
                f.write("blah")

            dataset_1_path = os.path.join(temporary_directory, "my_dataset_1")
            os.mkdir(dataset_1_path)

            with open(os.path.join(dataset_1_path, "my_file_1.txt"), "w") as f:
                f.write("blah")

            datasets = {"my_dataset_0": Dataset(dataset_0_path), "my_dataset_1": Dataset(dataset_1_path)}

            for dataset in datasets.values():
                dataset.update_local_metadata()

            manifest = Manifest(datasets=datasets, id="7e0025cd-bd68-4de6-b48d-2643ebd5effd", name="my-manifest")

            serialised_manifest = manifest.to_primitive()

            self.assertEqual(
                serialised_manifest,
                {
                    "id": manifest.id,
                    "name": "my-manifest",
                    "datasets": {
                        "my_dataset_0": dataset_0_path,
                        "my_dataset_1": dataset_1_path,
                    },
                },
            )

            deserialised_manifest = Manifest.deserialise(serialised_manifest)

        self.assertEqual(manifest.name, deserialised_manifest.name)
        self.assertEqual(manifest.id, deserialised_manifest.id)

        for key in manifest.datasets.keys():
            self.assertEqual(manifest.datasets[key].name, deserialised_manifest.datasets[key].name)
            self.assertEqual(manifest.datasets[key].id, deserialised_manifest.datasets[key].id)
            self.assertEqual(manifest.datasets[key].path, deserialised_manifest.datasets[key].path)

    def test_serialisation_and_deserialisation_with_datasets_instantiated_using_files_parameter(self):
        """Test that manifests can be serialised and deserialised when the datasets were instantiated using the `files`
        parameter.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            datasets = {
                "my_dataset_0": Dataset(
                    path=os.path.join(temporary_directory, "my_dataset_0"),
                    files=[Datafile(path=os.path.join(temporary_directory, "my_dataset_0", "my_file_0.txt"))],
                ),
                "my_dataset_1": Dataset(
                    path=os.path.join(temporary_directory, "my_dataset_1"),
                    files=[Datafile(path=os.path.join(temporary_directory, "my_dataset_1", "my_file_1.txt"))],
                ),
            }

            for dataset in datasets.values():
                dataset.update_local_metadata()

            manifest = Manifest(datasets=datasets, id="7e0025cd-bd68-4de6-b48d-2643ebd5effd", name="my-manifest")

            serialised_manifest = manifest.to_primitive()

            self.assertEqual(
                serialised_manifest,
                {
                    "id": manifest.id,
                    "name": "my-manifest",
                    "datasets": {
                        "my_dataset_0": manifest.datasets["my_dataset_0"].to_primitive(),
                        "my_dataset_1": manifest.datasets["my_dataset_1"].to_primitive(),
                    },
                },
            )

            deserialised_manifest = Manifest.deserialise(serialised_manifest)

        self.assertEqual(manifest.name, deserialised_manifest.name)
        self.assertEqual(manifest.id, deserialised_manifest.id)

        for key in manifest.datasets.keys():
            self.assertEqual(manifest.datasets[key].name, deserialised_manifest.datasets[key].name)
            self.assertEqual(manifest.datasets[key].id, deserialised_manifest.datasets[key].id)
            self.assertEqual(manifest.datasets[key].path, deserialised_manifest.datasets[key].path)

    def test_to_cloud(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset.tags = {"my": "tag"}
            dataset.files.filter(name="file_0.txt").one().tags = {"another": "tag"}
            dataset.upload(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-small-dataset"))

            manifest = Manifest(datasets={"my-dataset": dataset})
            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "manifest.json")
            manifest.to_cloud(cloud_path)

            persisted_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))
            self.assertEqual(persisted_manifest["datasets"]["my-dataset"], f"gs://{TEST_BUCKET_NAME}/my-small-dataset")

    def test_from_cloud(self):
        """Test that a Manifest can be instantiated from a cloud path."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_nice_dataset")
            dataset.upload(cloud_path=dataset_path)

            manifest = Manifest(datasets={"my-dataset": dataset})
            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-directory", "manifest.json")
            manifest.to_cloud(cloud_path)

            persisted_manifest = Manifest.from_cloud(cloud_path)

            self.assertEqual(persisted_manifest.id, manifest.id)
            self.assertEqual(persisted_manifest.hash_value, manifest.hash_value)

            self.assertEqual(
                {dataset.name for dataset in persisted_manifest.datasets.values()},
                {dataset.name for dataset in manifest.datasets.values()},
            )

            for dataset in persisted_manifest.datasets.values():
                self.assertEqual(dataset.path, dataset_path)
                self.assertTrue(len(dataset.files), 2)
                self.assertTrue(all(isinstance(file, Datafile) for file in dataset.files))

    def test_instantiating_from_serialised_cloud_datasets_with_no_dataset_json_file(self):
        """Test that a Manifest can be instantiated from a serialized cloud dataset with no `dataset.json` file. This
        simulates what happens when such a cloud dataset is referred to in a manifest received by a child service.
        """
        GoogleCloudStorageClient().upload_from_string(
            "[1, 2, 3]",
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset", "file_0.txt"),
        )

        GoogleCloudStorageClient().upload_from_string(
            "[4, 5, 6]",
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset", "file_1.txt"),
        )

        serialised_cloud_dataset = Dataset(path=f"gs://{TEST_BUCKET_NAME}/my_dataset").to_primitive()

        manifest = Manifest(datasets={"my_dataset": serialised_cloud_dataset})
        self.assertEqual(len(manifest.datasets), 1)
        self.assertEqual(manifest.datasets["my_dataset"].path, f"gs://{TEST_BUCKET_NAME}/my_dataset")
        self.assertEqual(len(manifest.datasets["my_dataset"].files), 2)

    def test_instantiating_from_datasets_from_different_cloud_buckets(self):
        """Test instantiating a manifest from multiple datasets from different cloud buckets."""
        storage_client = GoogleCloudStorageClient()

        extra_bucket_name = TEST_BUCKET_NAME + "-another"
        storage_client.create_bucket(name=extra_bucket_name)

        storage_client.upload_from_string(
            "[1, 2, 3]",
            storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset_a", "file_0.txt"),
        )

        storage_client.upload_from_string(
            "[4, 5, 6]", storage.path.generate_gs_path(extra_bucket_name, "my_dataset_b", "the_data.txt")
        )

        manifest = Manifest(
            datasets={
                "my_dataset_a": f"gs://{TEST_BUCKET_NAME}/my_dataset_a",
                "my_dataset_b": f"gs://{extra_bucket_name}/my_dataset_b",
            }
        )

        self.assertEqual({dataset.name for dataset in manifest.datasets.values()}, {"my_dataset_a", "my_dataset_b"})

        files = [list(dataset.files)[0] for dataset in manifest.datasets.values()]
        self.assertEqual({file.bucket_name for file in files}, {TEST_BUCKET_NAME, extra_bucket_name})

    def test_instantiating_from_multiple_local_datasets(self):
        """Test instantiating a manifest from multiple local datasets."""
        manifest = Manifest(
            datasets={
                "dataset_0": os.path.join("path", "to", "dataset_0"),
                "dataset_1": os.path.join("path", "to", "dataset_1"),
            },
        )

        self.assertEqual({dataset.name for dataset in manifest.datasets.values()}, {"dataset_0", "dataset_1"})

    def test_update_dataset_paths(self):
        """Test that dataset paths can be updated by providing a callable to `Dataset.update_dataset_paths`."""
        manifest = Manifest(
            datasets={
                "dataset_0": os.path.join("path", "to", "dataset_0"),
                "dataset_1": os.path.join("path", "to", "dataset_1"),
                "dataset_2": os.path.join("path", "to", "dataset_2"),
            },
        )

        def path_generator(dataset):
            if dataset.path.endswith("0"):
                return dataset.path.replace("0", "zero")
            elif dataset.path.endswith("1"):
                return dataset.path.replace("1", "one")
            return dataset.path

        manifest.update_dataset_paths(path_generator)

        # Check the first two datasets' paths are updated.
        self.assertEqual(manifest.datasets["dataset_0"].path, os.path.join("path", "to", "dataset_zero"))
        self.assertEqual(manifest.datasets["dataset_1"].path, os.path.join("path", "to", "dataset_one"))

        # Check the third dataset's path is unmodified.
        self.assertEqual(manifest.datasets["dataset_2"].path, os.path.join("path", "to", "dataset_2"))

    def test_ignore_stored_metadata(self):
        """Test that a manifest's datasets' and datafiles' metadata can be ignored."""
        dataset_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "yet_another_dataset")

        with Dataset(dataset_cloud_path) as dataset:
            dataset.labels = ["wham", "bam", "slam"]

        datafile_cloud_path = storage.path.join(dataset_cloud_path, "another-file.dat")

        with Datafile(datafile_cloud_path, mode="w") as (datafile, f):
            f.write("Zapppppp")
            datafile.tags = {"hippy": "dippy"}

        with patch("octue.resources.Dataset._use_cloud_metadata") as mock_dataset_use_cloud_metadata:
            with patch("octue.resources.Datafile._use_cloud_metadata") as mock_datafile_use_cloud_metadata:
                manifest = Manifest(datasets={"my-dataset": dataset_cloud_path}, ignore_stored_metadata=True)

        dataset_in_manifest = manifest.datasets["my-dataset"]
        datafile_in_manifest = dataset_in_manifest.files.one()

        # Test that the manifest's dataset's files are included.
        self.assertEqual(datafile_in_manifest.cloud_path, datafile_cloud_path)

        # Test that the manifest's dataset's and datafile's metadata are ignored.
        mock_datafile_use_cloud_metadata.assert_not_called()
        self.assertEqual(datafile_in_manifest.tags, {})

        mock_dataset_use_cloud_metadata.assert_not_called()
        self.assertEqual(dataset_in_manifest.labels, set())

    def test_use_signed_urls_for_datasets(self):
        """Test that cloud URI dataset paths in a manifest can be swapped for signed URLs."""
        manifest = Manifest(datasets={"my_dataset": self.create_nested_cloud_dataset()})
        self.assertTrue(manifest.datasets["my_dataset"].path.startswith("gs://"))

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", mock_generate_signed_url):
            manifest.use_signed_urls_for_datasets()

        self.assertTrue(manifest.datasets["my_dataset"].path.startswith("http"))
        self.assertIn(".signed_metadata_files", manifest.datasets["my_dataset"].path)

    def test_use_signed_urls_for_datasets_is_idempotent(self):
        """Test that calling `use_signed_urls_for_datasets` on a manifest that already has signed URLs for its datasets'
        paths just leaves the paths as they are.
        """
        manifest = Manifest(datasets={"my_dataset": self.create_nested_cloud_dataset()})

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", mock_generate_signed_url):
            manifest.use_signed_urls_for_datasets()
            manifest.use_signed_urls_for_datasets()

        self.assertTrue(manifest.datasets["my_dataset"].path.startswith("http"))
        self.assertIn(".signed_metadata_files", manifest.datasets["my_dataset"].path)
