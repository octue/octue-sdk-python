import copy
import json
import os
import tempfile

from octue.resources import Datafile, Dataset, Manifest
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase


class TestManifest(BaseTestCase):
    def test_hash_value(self):
        """ Test hashing a manifest with multiple datasets gives a hash of length 128. """
        manifest = self.create_valid_manifest()
        hash_ = manifest.hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 64)

    def test_hashes_for_the_same_manifest_are_the_same(self):
        """ Ensure the hashes for two manifests that are exactly the same are the same."""
        first_file = self.create_valid_manifest()
        second_file = copy.deepcopy(first_file)
        self.assertEqual(first_file.hash_value, second_file.hash_value)

    def test_to_cloud(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance. """
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as output_directory:
            file_0_path = os.path.join(output_directory, "file_0.txt")
            file_1_path = os.path.join(output_directory, "file_1.txt")

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
                path_to_manifest_file=storage.path.join(output_directory, "manifest.json"),
            )

            persisted_manifest = json.loads(
                GoogleCloudStorageClient(project_name).download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(output_directory, "manifest.json"),
                )
            )

            self.assertEqual(persisted_manifest["datasets"], [f"gs://octue-test-bucket{output_directory}/my-dataset"])
            self.assertEqual(persisted_manifest["keys"], {"my-dataset": 0})

    def test_to_cloud_without_storing_datasets(self):
        """Test that a manifest can be uploaded to the cloud as a serialised JSON file of the Manifest instance. """
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as output_directory:
            file_0_path = os.path.join(output_directory, "file_0.txt")
            file_1_path = os.path.join(output_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                name="my-dataset",
                path=output_directory,
                files={
                    Datafile(timestamp=None, path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=None, path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})

            manifest.to_cloud(
                project_name,
                TEST_BUCKET_NAME,
                path_to_manifest_file=storage.path.join(output_directory, "manifest.json"),
                store_datasets=False,
            )

            persisted_manifest = json.loads(
                GoogleCloudStorageClient(project_name).download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(output_directory, "manifest.json"),
                )
            )

            self.assertEqual(persisted_manifest["datasets"], [output_directory])
            self.assertEqual(persisted_manifest["keys"], {"my-dataset": 0})

    def test_from_cloud(self):
        """Test that a Manifest can be instantiated from the cloud."""
        project_name = "test-project"

        with tempfile.TemporaryDirectory() as output_directory:
            file_0_path = os.path.join(output_directory, "file_0.txt")
            file_1_path = os.path.join(output_directory, "file_1.txt")

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
                path_to_manifest_file=storage.path.join(output_directory, "manifest.json"),
            )

        persisted_manifest = Manifest.from_cloud(
            project_name=project_name,
            bucket_name=TEST_BUCKET_NAME,
            path_to_manifest_file=storage.path.join(output_directory, "manifest.json"),
        )

        self.assertEqual(persisted_manifest.path, f"gs://{TEST_BUCKET_NAME}{output_directory}/manifest.json")
        self.assertEqual(persisted_manifest.id, manifest.id)
        self.assertEqual(persisted_manifest.hash_value, manifest.hash_value)
        self.assertEqual(persisted_manifest.keys, manifest.keys)
        self.assertEqual(
            {dataset.name for dataset in persisted_manifest.datasets}, {dataset.name for dataset in manifest.datasets}
        )

        for dataset in persisted_manifest.datasets:
            self.assertEqual(dataset.path, f"gs://{TEST_BUCKET_NAME}{output_directory}/{dataset.name}")
            self.assertTrue(len(dataset.files), 2)
            self.assertTrue(all(isinstance(file, Datafile) for file in dataset.files))
