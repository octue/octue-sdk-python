import copy
import json
import os
import tempfile

from octue.cloud import storage
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

        files = [
            Datafile(path="gs://hello/file.txt", hypothetical=True),
            Datafile(path="gs://goodbye/file.csv", hypothetical=True),
        ]

        manifest = Manifest(datasets={"my_dataset": Dataset(files=files)})
        self.assertTrue(manifest.all_datasets_are_in_cloud)

    def test_deserialise(self):
        """Test that manifests can be deserialised."""
        manifest = self.create_valid_manifest()
        serialised_manifest = manifest.to_primitive()
        deserialised_manifest = Manifest.deserialise(serialised_manifest)

        self.assertEqual(manifest.name, deserialised_manifest.name)
        self.assertEqual(manifest.id, deserialised_manifest.id)
        self.assertEqual(manifest.absolute_path, deserialised_manifest.absolute_path)

        for key in manifest.datasets.keys():
            self.assertEqual(manifest.datasets[key].name, deserialised_manifest.datasets[key].name)
            self.assertEqual(manifest.datasets[key].id, deserialised_manifest.datasets[key].id)
            self.assertEqual(manifest.datasets[key].absolute_path, deserialised_manifest.datasets[key].absolute_path)

    def test_to_cloud(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance via
        (`bucket_name`, `output_directory`) and via `gs_path`.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset.to_cloud(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-small-dataset"))

            manifest = Manifest(datasets={"my-dataset": dataset})
            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "manifest.json")
            manifest.to_cloud(cloud_path)

            persisted_manifest = json.loads(GoogleCloudStorageClient().download_as_string(cloud_path))

            self.assertEqual(persisted_manifest["datasets"]["my-dataset"], "gs://octue-test-bucket/my-small-dataset")

    def test_to_cloud_without_storing_datasets(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            manifest = Manifest(datasets={"my-dataset": dataset})

            manifest.to_cloud(
                storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-manifests", "manifest.json"),
                store_datasets=False,
            )

        persisted_manifest = json.loads(
            GoogleCloudStorageClient().download_as_string(
                storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-manifests", "manifest.json")
            )
        )

        self.assertEqual(persisted_manifest["datasets"]["my-dataset"], temporary_directory)

    def test_from_cloud(self):
        """Test that a Manifest can be instantiated from the cloud via (`bucket_name`, `output_directory`) and via
        `gs_path`.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_nice_dataset")
            dataset.to_cloud(cloud_path=dataset_path)

            manifest = Manifest(datasets={"my-dataset": dataset})
            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-directory", "manifest.json")
            manifest.to_cloud(cloud_path)

            persisted_manifest = Manifest.from_cloud(cloud_path)

            self.assertEqual(persisted_manifest.path, f"gs://{TEST_BUCKET_NAME}/my-directory/manifest.json")
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

        serialised_cloud_dataset = Dataset.from_cloud(cloud_path=f"gs://{TEST_BUCKET_NAME}/my_dataset").to_primitive()

        manifest = Manifest(datasets={"my_dataset": serialised_cloud_dataset})
        self.assertEqual(len(manifest.datasets), 1)
        self.assertEqual(manifest.datasets["my_dataset"].path, f"gs://{TEST_BUCKET_NAME}/my_dataset")
        self.assertEqual(len(manifest.datasets["my_dataset"].files), 2)

    def test_instantiating_from_datasets_from_different_cloud_buckets(self):
        """Test instantiating a manifest from multiple datasets from different cloud buckets."""
        storage_client = GoogleCloudStorageClient()
        storage_client.create_bucket(name="another-test-bucket")

        storage_client.upload_from_string(
            "[1, 2, 3]",
            storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset_a", "file_0.txt"),
        )

        storage_client.upload_from_string(
            "[4, 5, 6]", storage.path.generate_gs_path("another-test-bucket", "my_dataset_b", "the_data.txt")
        )

        manifest = Manifest(
            datasets={
                "my_dataset_a": f"gs://{TEST_BUCKET_NAME}/my_dataset_a",
                "my_dataset_b": "gs://another-test-bucket/my_dataset_b",
            }
        )

        self.assertEqual({dataset.name for dataset in manifest.datasets.values()}, {"my_dataset_a", "my_dataset_b"})

        files = [list(dataset.files)[0] for dataset in manifest.datasets.values()]
        self.assertEqual({file.bucket_name for file in files}, {TEST_BUCKET_NAME, "another-test-bucket"})

    def test_instantiating_from_multiple_local_datasets(self):
        """Test instantiating a manifest from multiple local datasets."""
        manifest = Manifest(
            datasets={
                "dataset_0": os.path.join("path", "to", "dataset_0"),
                "dataset_1": os.path.join("path", "to", "dataset_1"),
            },
        )

        self.assertEqual({dataset.name for dataset in manifest.datasets.values()}, {"dataset_0", "dataset_1"})

    def test_deprecation_warning_issued_if_datasets_provided_as_list(self):
        """Test that, if datasets are provided as a list (the old format), a deprecation warning is issued and the list
        is converted to a dictionary (the new format).
        """
        test_cases = [
            {
                "datasets": [f"gs://{TEST_BUCKET_NAME}/my_dataset_1", f"gs://{TEST_BUCKET_NAME}/my_dataset_2"],
                "keys": {"my_dataset_1": 0, "my_dataset_2": 1},
                "expected_keys": {"my_dataset_1", "my_dataset_2"},
            },
            {
                "datasets": [f"gs://{TEST_BUCKET_NAME}/my_dataset_1", f"gs://{TEST_BUCKET_NAME}/my_dataset_2"],
                "expected_keys": {"dataset_0", "dataset_1"},
            },
            {
                "datasets": [Dataset(name="wind_speed_map"), Dataset(name="elevation_map")],
                "keys": {"elevation_map": 1, "wind_speed_map": 0},
                "expected_keys": {"elevation_map", "wind_speed_map"},
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case=test_case):
                with self.assertWarns(DeprecationWarning):
                    manifest = Manifest(datasets=test_case["datasets"], keys=test_case.get("keys"))

                self.assertEqual(set(manifest.datasets.keys()), test_case["expected_keys"])
