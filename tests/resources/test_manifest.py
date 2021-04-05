import copy
import json
import os
import tempfile

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Datafile, Dataset, Manifest
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase


class TestManifest(BaseTestCase):
    def test_hash_value(self):
        """Test hashing a manifest with multiple datasets gives a hash of length 8."""
        manifest = self.create_valid_manifest()
        hash_ = manifest.hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 8)

    def test_hashes_for_the_same_manifest_are_the_same(self):
        """ Ensure the hashes for two manifests that are exactly the same are the same."""
        first_manifest = self.create_valid_manifest()
        second_manifest = copy.deepcopy(first_manifest)
        self.assertEqual(first_manifest.hash_value, second_manifest.hash_value)

    def test_all_datasets_are_in_cloud(self):
        """Test whether all files of all datasets in a manifest are in the cloud or not can be determined."""
        self.assertFalse(Manifest().all_datasets_are_in_cloud())
        self.assertFalse(self.create_valid_manifest().all_datasets_are_in_cloud())

        files = [
            Datafile(timestamp=None, path="gs://hello/file.txt"),
            Datafile(timestamp=None, path="gs://goodbye/file.csv"),
        ]

        manifest = Manifest(datasets=[Dataset(files=files)], keys={"my_dataset": 0})
        self.assertTrue(manifest.all_datasets_are_in_cloud())

    def test_to_cloud(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance. """
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")
            file_1_path = os.path.join(temporary_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                name="my-dataset",
                files={
                    Datafile(timestamp=None, path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=None, path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})

            manifest.to_cloud(
                project_name,
                TEST_BUCKET_NAME,
                path_to_manifest_file=storage.path.join("blah", "manifest.json"),
            )

        persisted_manifest = json.loads(
            GoogleCloudStorageClient(project_name).download_as_string(
                bucket_name=TEST_BUCKET_NAME,
                path_in_bucket=storage.path.join("blah", "manifest.json"),
            )
        )

        self.assertEqual(persisted_manifest["datasets"], ["gs://octue-test-bucket/blah/my-dataset"])
        self.assertEqual(persisted_manifest["keys"], {"my-dataset": 0})

    def test_to_cloud_without_storing_datasets(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance. """
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")
            file_1_path = os.path.join(temporary_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                name="my-dataset",
                path=temporary_directory,
                files={
                    Datafile(timestamp=None, path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=None, path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})

            manifest.to_cloud(
                project_name,
                TEST_BUCKET_NAME,
                path_to_manifest_file=storage.path.join("my-manifests", "manifest.json"),
                store_datasets=False,
            )

        persisted_manifest = json.loads(
            GoogleCloudStorageClient(project_name).download_as_string(
                bucket_name=TEST_BUCKET_NAME,
                path_in_bucket=storage.path.join("my-manifests", "manifest.json"),
            )
        )

        self.assertEqual(persisted_manifest["datasets"], [temporary_directory])
        self.assertEqual(persisted_manifest["keys"], {"my-dataset": 0})

    def test_from_cloud(self):
        """Test that a Manifest can be instantiated from the cloud."""
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")
            file_1_path = os.path.join(temporary_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                name="my-dataset",
                files={
                    Datafile(timestamp=None, path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=None, path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})
            manifest.to_cloud(
                project_name,
                TEST_BUCKET_NAME,
                path_to_manifest_file=storage.path.join("my-directory", "manifest.json"),
            )

        persisted_manifest = Manifest.from_cloud(
            project_name=project_name,
            bucket_name=TEST_BUCKET_NAME,
            path_to_manifest_file=storage.path.join("my-directory", "manifest.json"),
        )

        self.assertEqual(persisted_manifest.path, f"gs://{TEST_BUCKET_NAME}/my-directory/manifest.json")
        self.assertEqual(persisted_manifest.id, manifest.id)
        self.assertEqual(persisted_manifest.hash_value, manifest.hash_value)
        self.assertEqual(persisted_manifest.keys, manifest.keys)
        self.assertEqual(
            {dataset.name for dataset in persisted_manifest.datasets}, {dataset.name for dataset in manifest.datasets}
        )

        for dataset in persisted_manifest.datasets:
            self.assertEqual(dataset.path, f"gs://{TEST_BUCKET_NAME}/my-directory/{dataset.name}")
            self.assertTrue(len(dataset.files), 2)
            self.assertTrue(all(isinstance(file, Datafile) for file in dataset.files))
