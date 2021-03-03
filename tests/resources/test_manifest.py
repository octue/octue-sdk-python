import copy
import os
import tempfile
import time

from octue.resources import Datafile, Dataset, Manifest
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
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
        bucket_name = os.environ["TEST_BUCKET_NAME"]
        output_directory = "my_datasets"

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
                    Datafile(timestamp=time.time(), path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=time.time(), path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})

            manifest.to_cloud(project_name, bucket_name, output_directory)

            persisted_manifest = GoogleCloudStorageClient(project_name).download_as_string(
                bucket_name=bucket_name,
                path_in_bucket=storage.path.join(output_directory, "manifest.json"),
            )

            self.assertEqual(manifest.serialise(shallow=True, to_string=True), persisted_manifest)

    def test_from_cloud(self):
        """Test that a Manifest can be instantiated from the cloud."""
        project_name = "test-project"
        bucket_name = os.environ["TEST_BUCKET_NAME"]

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
                    Datafile(timestamp=time.time(), path=file_0_path, sequence=0, tags={"hello"}),
                    Datafile(timestamp=time.time(), path=file_1_path, sequence=1, tags={"goodbye"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})
            manifest.to_cloud(project_name, bucket_name, output_directory)

        persisted_manifest = Manifest.from_cloud(
            project_name=project_name,
            bucket_name=bucket_name,
            directory_path=output_directory,
        )

        self.assertEqual(persisted_manifest.path, f"gs://{bucket_name}{output_directory}")
        self.assertEqual(persisted_manifest.id, manifest.id)
        self.assertEqual(persisted_manifest.hash_value, manifest.hash_value)
        self.assertEqual(persisted_manifest.keys, manifest.keys)
        self.assertEqual(
            {dataset.name for dataset in persisted_manifest.datasets}, {dataset.name for dataset in manifest.datasets}
        )

        for dataset in persisted_manifest.datasets:
            self.assertEqual(dataset.path, f"gs://{bucket_name}{output_directory}/{dataset.name}")
            self.assertTrue(len(dataset.files), 2)
            self.assertTrue(all(isinstance(file, Datafile) for file in dataset.files))

    def test_from_cloud_only_collects_its_own_datasets(self):
        """Test that instantiating a Manifest from the cloud ignores datasets outside of its own."""
        project_name = "test-project"
        bucket_name = os.environ["TEST_BUCKET_NAME"]

        with tempfile.TemporaryDirectory() as output_directory:
            file_0_path = os.path.join(output_directory, "file_0.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            dataset = Dataset(
                name="my-dataset",
                files={
                    Datafile(timestamp=time.time(), path=file_0_path, sequence=0, tags={"hello"}),
                },
            )

            manifest = Manifest(datasets=[dataset], keys={"my-dataset": 0})
            manifest.to_cloud(project_name, bucket_name, output_directory)

            another_dataset = Dataset(
                name="dataset-not-to-touch",
                files={Datafile(timestamp=time.time(), path=file_0_path, sequence=0, tags={"hello"})},
            )

            another_dataset.to_cloud(project_name, bucket_name, output_directory)

        persisted_manifest = Manifest.from_cloud(
            project_name=project_name,
            bucket_name=bucket_name,
            directory_path=output_directory,
        )

        self.assertEqual(len(persisted_manifest.datasets), 1)
        self.assertTrue(another_dataset.name not in [dataset.name for dataset in persisted_manifest.datasets])
