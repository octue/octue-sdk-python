import copy
import json
import os
import tempfile
import warnings

from octue import definitions, exceptions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Datafile, Dataset
from octue.resources.filter_containers import FilterSet
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestDataset(BaseTestCase):
    def _create_nested_cloud_dataset(self, dataset_name="a_dataset"):
        cloud_storage_client = GoogleCloudStorageClient(TEST_PROJECT_NAME)

        cloud_storage_client.upload_from_string(
            "[1, 2, 3]", bucket_name=TEST_BUCKET_NAME, path_in_bucket=f"{dataset_name}/file_0.txt"
        )

        cloud_storage_client.upload_from_string(
            "[4, 5, 6]", bucket_name=TEST_BUCKET_NAME, path_in_bucket=f"{dataset_name}/file_1.txt"
        )

        cloud_storage_client.upload_from_string(
            "['a', 'b', 'c']", bucket_name=TEST_BUCKET_NAME, path_in_bucket=f"{dataset_name}/sub-directory/sub_file.txt"
        )

        cloud_storage_client.upload_from_string(
            "['blah', 'b', 'c']",
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=f"{dataset_name}/sub-directory/sub-sub-directory/sub_sub_file.txt",
        )

    def test_instantiates_with_no_args(self):
        """Ensures a Datafile instantiates using only a path and generates a uuid ID"""
        Dataset()

    def test_instantiates_with_kwargs(self):
        """Ensures that keyword arguments can be used to construct the dataset initially"""
        files = [Datafile(path="path-within-dataset/a_test_file.csv")]
        resource = Dataset(files=files, labels="one two")
        self.assertEqual(len(resource.files), 1)

    def test_len(self):
        """Test that the length of a Dataset is the number of files it contains."""
        dataset = self.create_valid_dataset()
        self.assertEqual(len(dataset), len(dataset.files))

    def test_iter(self):
        """Test that iterating over a Dataset is equivalent to iterating over its files."""
        dataset = self.create_valid_dataset()
        iterated_files = {file for file in dataset}
        self.assertEqual(iterated_files, dataset.files)

    def test_using_append_raises_deprecation_warning(self):
        """Test that Dataset.append is deprecated but gets redirected to Dataset.add."""
        resource = Dataset()

        with warnings.catch_warnings(record=True) as warning:
            resource.append(Datafile(path="path-within-dataset/a_test_file.csv"))
            self.assertEqual(len(warning), 1)
            self.assertTrue(issubclass(warning[-1].category, DeprecationWarning))
            self.assertIn("deprecated", str(warning[-1].message))
            self.assertEqual(len(resource.files), 1)

    def test_add_single_file_to_empty_dataset(self):
        """Ensures that when a dataset is empty, it can be added to"""
        resource = Dataset()
        resource.add(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 1)

    def test_add_single_file_to_existing_dataset(self):
        """Ensures that when a dataset is not empty, it can be added to"""
        files = [Datafile(path="path-within-dataset/a_test_file.csv")]
        resource = Dataset(files=files, labels="one two", tags={"a": "b"})
        resource.add(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 2)

    def test_add_with_datafile_creation_shortcut(self):
        """Ensures that when a dataset is not empty, it can be added to"""
        resource = Dataset()
        resource.add(path="path-within-dataset/a_test_file.csv")
        self.assertEqual(len(resource.files), 1)

    def test_add_multiple_files(self):
        """Ensures that when a dataset is not empty, it can be added to"""
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.csv"),
        ]
        resource = Dataset()
        resource.add(*files)
        self.assertEqual(len(resource.files), 2)

    def test_cannot_add_non_datafiles(self):
        """Ensures that exception will be raised if adding a non-datafile object"""

        class NotADatafile:
            pass

        resource = Dataset()
        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.add(NotADatafile())

        self.assertIn("must be of class Datafile to add it to a Dataset", e.exception.args[0])

    def test_filter_catches_single_underscore_mistake(self):
        """Ensure that if the filter name contains only single underscores, an error is raised."""
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )

        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.files.filter(name_icontains="Test")

        self.assertIn("Invalid filter name 'name_icontains'. Filter names should be in the form", e.exception.args[0])

    def test_filter_name_contains(self):
        """Ensures that filter works with the name_contains and name_icontains lookups"""
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.files.filter(name__icontains="Test")
        self.assertEqual(2, len(files))
        files = resource.files.filter(name__icontains="A")
        self.assertEqual(2, len(files))
        files = resource.files.filter(name__contains="Test")
        self.assertEqual(1, len(files))
        files = resource.files.filter(name__icontains="test")
        self.assertEqual(2, len(files))
        files = resource.files.filter(name__icontains="file")
        self.assertEqual(2, len(files))

    def test_filter_name_with(self):
        """Ensures that filter works with the name_endswith and name_startswith lookups"""
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv"),
                Datafile(path="path-within-dataset/a_your_file.csv"),
            ]
        )
        files = resource.files.filter(name__starts_with="a_my")
        self.assertEqual(1, len(files))
        files = resource.files.filter(name__starts_with="a_your")
        self.assertEqual(1, len(files))
        files = resource.files.filter(name__starts_with="a_")
        self.assertEqual(2, len(files))
        files = resource.files.filter(name__starts_with="b")
        self.assertEqual(0, len(files))
        files = resource.files.filter(name__ends_with="_file.csv")
        self.assertEqual(2, len(files))
        files = resource.files.filter(name__ends_with="r_file.csv")
        self.assertEqual(1, len(files))
        files = resource.files.filter(name__ends_with="y_file.csv")
        self.assertEqual(1, len(files))
        files = resource.files.filter(name__ends_with="other.csv")
        self.assertEqual(0, len(files))

    def test_filter_by_label(self):
        """Ensures that filter works with label lookups"""
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", labels="one a2 b3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", labels="two a2 b3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", labels="three all"),
            ]
        )

        files = resource.files.filter(labels__contains="a")
        self.assertEqual(0, len(files))
        files = resource.files.filter(labels__contains="one")
        self.assertEqual(1, len(files))
        files = resource.files.filter(labels__contains="all")
        self.assertEqual(3, len(files))
        files = resource.files.filter(labels__any_label_starts_with="b")
        self.assertEqual(2, len(files))
        files = resource.files.filter(labels__any_label_ends_with="3")
        self.assertEqual(2, len(files))
        # files = resource.files.filter(labels__contains="hre")
        # self.assertEqual(1, len(files))

    def test_get_file_by_label(self):
        """Ensure files can be accessed by label from the dataset."""
        files = [
            Datafile(path="path-within-dataset/a_my_file.csv", labels="one a b3 all"),
            Datafile(path="path-within-dataset/a_your_file.csv", labels="two a2 b3 all"),
            Datafile(path="path-within-dataset/a_your_file.csv", labels="three all"),
        ]

        resource = Dataset(files=files)

        # Check working for single result
        self.assertIs(resource.get_file_by_label("three"), files[2])

        # Check raises for too many results
        with self.assertRaises(exceptions.UnexpectedNumberOfResultsException) as e:
            resource.get_file_by_label("all")

        self.assertIn("More than one result found", e.exception.args[0])

        # Check raises for no result
        with self.assertRaises(exceptions.UnexpectedNumberOfResultsException) as e:
            resource.get_file_by_label("billyjeanisnotmylover")

        self.assertIn("No results found for filters {'labels__contains': 'billyjeanisnotmylover'}", e.exception.args[0])

    def test_filter_name_filters_include_extension(self):
        """Ensures that filters applied to the name will catch terms in the extension"""
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.txt"),
        ]

        self.assertEqual(Dataset(files=files).files.filter(name__icontains="txt"), FilterSet({files[1]}))

    def test_filter_name_filters_exclude_path(self):
        """Ensures that filters applied to the name will not catch terms in the extension"""
        resource = Dataset(
            files=[
                Datafile(path="first-path-within-dataset/a_test_file.csv"),
                Datafile(path="second-path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.files.filter(name__icontains="second")
        self.assertEqual(0, len(files))

    def test_using_get_files_raises_deprecation_warning(self):
        """Test that Dataset.get_files is deprecated but gets redirected to Dataset.files.filter."""
        resource = Dataset(
            files=[
                Datafile(path="first-path-within-dataset/a_test_file.csv"),
                Datafile(path="second-path-within-dataset/a_test_file.txt"),
            ]
        )

        with warnings.catch_warnings(record=True) as warning:
            filtered_files = resource.get_files(name__icontains="second")
            self.assertIn("deprecated", str(warning[-1].message))
            self.assertEqual(len(filtered_files), 0)

    def test_hash_value(self):
        """Test hashing a dataset with multiple files gives a hash of length 8."""
        hash_ = self.create_valid_dataset().hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 8)

    def test_hashes_for_the_same_dataset_are_the_same(self):
        """Ensure the hashes for two datasets that are exactly the same are the same."""
        first_dataset = self.create_valid_dataset()
        second_dataset = copy.deepcopy(first_dataset)
        self.assertEqual(first_dataset.hash_value, second_dataset.hash_value)

    def test_serialise(self):
        """Test that a dataset can be serialised."""
        dataset = self.create_valid_dataset()
        self.assertEqual(len(dataset.to_primitive()["files"]), 2)

    def test_exists_in_cloud(self):
        """Test whether all files of a dataset are in the cloud or not can be determined."""
        self.assertFalse(Dataset().all_files_are_in_cloud)
        self.assertFalse(self.create_valid_dataset().all_files_are_in_cloud)

        files = [
            Datafile(path="gs://hello/file.txt", project_name="blah", hypothetical=True),
            Datafile(path="gs://goodbye/file.csv", project_name="blah", hypothetical=True),
        ]

        self.assertTrue(Dataset(files=files).all_files_are_in_cloud)

    def test_from_cloud(self):
        """Test that a Dataset in cloud storage can be accessed via (`bucket_name`, `output_directory`) and via
        `gs_path`.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")
            file_1_path = os.path.join(temporary_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                name="dataset_0",
                files={
                    Datafile(path=file_0_path, labels={"hello"}, tags={"a": "b"}),
                    Datafile(path=file_1_path, labels={"goodbye"}, tags={"a": "b"}),
                },
                tags={"a": "b", "c": 1},
            )

            dataset.to_cloud(
                project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, output_directory="a_directory"
            )

            bucket_name = TEST_BUCKET_NAME
            path_to_dataset_directory = storage.path.join("a_directory", dataset.name)
            gs_path = f"gs://{bucket_name}/{path_to_dataset_directory}"

            for location_parameters in (
                {
                    "bucket_name": bucket_name,
                    "path_to_dataset_directory": path_to_dataset_directory,
                    "cloud_path": None,
                },
                {"bucket_name": None, "path_to_dataset_directory": None, "cloud_path": gs_path},
            ):

                persisted_dataset = Dataset.from_cloud(
                    project_name=TEST_PROJECT_NAME,
                    **location_parameters,
                )

                self.assertEqual(persisted_dataset.path, f"gs://{TEST_BUCKET_NAME}/a_directory/{dataset.name}")
                self.assertEqual(persisted_dataset.id, dataset.id)
                self.assertEqual(persisted_dataset.name, dataset.name)
                self.assertEqual(persisted_dataset.hash_value, dataset.hash_value)
                self.assertEqual(persisted_dataset.tags, dataset.tags)
                self.assertEqual(persisted_dataset.labels, dataset.labels)
                self.assertEqual({file.name for file in persisted_dataset.files}, {file.name for file in dataset.files})

                for file in persisted_dataset:
                    self.assertEqual(file.path, f"gs://{TEST_BUCKET_NAME}/a_directory/{dataset.name}/{file.name}")

    def test_from_cloud_with_no_datafile_json_file(self):
        """Test that any cloud directory can be accessed as a dataset if it has no `dataset.json` metadata file in it,
        the cloud dataset doesn't lose any information during serialization, and a metadata file is uploaded afterwards.
        """
        cloud_storage_client = GoogleCloudStorageClient(TEST_PROJECT_NAME)

        cloud_storage_client.upload_from_string(
            "[1, 2, 3]", bucket_name=TEST_BUCKET_NAME, path_in_bucket="my_dataset/file_0.txt"
        )

        cloud_storage_client.upload_from_string(
            "[4, 5, 6]", bucket_name=TEST_BUCKET_NAME, path_in_bucket="my_dataset/file_1.txt"
        )

        cloud_dataset = Dataset.from_cloud(
            project_name=TEST_PROJECT_NAME,
            cloud_path=f"gs://{TEST_BUCKET_NAME}/my_dataset",
        )

        self.assertEqual(cloud_dataset.path, f"gs://{TEST_BUCKET_NAME}/my_dataset")
        self.assertEqual(cloud_dataset.name, "my_dataset")
        self.assertEqual({file.name for file in cloud_dataset.files}, {"file_0.txt", "file_1.txt"})

        for file in cloud_dataset:
            self.assertEqual(file.path, f"gs://{TEST_BUCKET_NAME}/my_dataset/{file.name}")

        # Test serialisation doesn't lose any information.
        deserialised_dataset = Dataset.deserialise(cloud_dataset.to_primitive())
        self.assertEqual(deserialised_dataset.id, cloud_dataset.id)
        self.assertEqual(deserialised_dataset.name, cloud_dataset.name)
        self.assertEqual(deserialised_dataset.path, cloud_dataset.path)
        self.assertEqual(deserialised_dataset.hash_value, cloud_dataset.hash_value)

        # Test dataset metadata file has been uploaded.
        dataset_metadata = json.loads(
            cloud_storage_client.download_as_string(
                cloud_path=storage.path.join(cloud_dataset.path, definitions.DATASET_METADATA_FILENAME)
            )
        )
        del dataset_metadata["id"]

        self.assertEqual(
            dataset_metadata,
            {
                "files": [
                    "gs://octue-test-bucket/my_dataset/file_0.txt",
                    "gs://octue-test-bucket/my_dataset/file_1.txt",
                ],
                "labels": [],
                "name": "my_dataset",
                "tags": {},
            },
        )

    def test_from_cloud_with_nested_dataset_and_no_datafile_json_file(self):
        """Test that a nested dataset is loaded from the cloud correctly."""
        self._create_nested_cloud_dataset()

        cloud_dataset = Dataset.from_cloud(
            project_name=TEST_PROJECT_NAME,
            cloud_path=f"gs://{TEST_BUCKET_NAME}/a_dataset",
            recursive=True,
        )

        self.assertEqual(cloud_dataset.path, f"gs://{TEST_BUCKET_NAME}/a_dataset")
        self.assertEqual(cloud_dataset.name, "a_dataset")
        self.assertEqual(
            {file.name for file in cloud_dataset.files},
            {"file_0.txt", "file_1.txt", "sub_file.txt", "sub_sub_file.txt"},
        )

        # Test dataset metadata file has been uploaded.
        dataset_metadata = json.loads(
            GoogleCloudStorageClient(TEST_PROJECT_NAME).download_as_string(
                cloud_path=storage.path.join(cloud_dataset.path, definitions.DATASET_METADATA_FILENAME)
            )
        )
        del dataset_metadata["id"]

        self.assertEqual(
            set(dataset_metadata["files"]),
            {
                "gs://octue-test-bucket/a_dataset/file_0.txt",
                "gs://octue-test-bucket/a_dataset/file_1.txt",
                "gs://octue-test-bucket/a_dataset/sub-directory/sub_file.txt",
                "gs://octue-test-bucket/a_dataset/sub-directory/sub-sub-directory/sub_sub_file.txt",
            },
        )

    def test_to_cloud(self):
        """Test that a dataset can be uploaded to the cloud via (`bucket_name`, `output_directory`) and via `gs_path`,
        including all its files and a serialised JSON file of the Datafile instance.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")
            file_1_path = os.path.join(temporary_directory, "file_1.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            with open(file_1_path, "w") as f:
                f.write("[4, 5, 6]")

            dataset = Dataset(
                files={
                    Datafile(path=file_0_path, labels={"hello"}),
                    Datafile(path=file_1_path, labels={"goodbye"}),
                },
                tags={"a": "b", "c": 1},
            )

            bucket_name = TEST_BUCKET_NAME
            output_directory = "my_datasets"
            cloud_path = storage.path.generate_gs_path(bucket_name, output_directory)

            for location_parameters in (
                {"bucket_name": bucket_name, "output_directory": output_directory, "cloud_path": None},
                {"bucket_name": None, "output_directory": None, "cloud_path": cloud_path},
            ):
                dataset.to_cloud(TEST_PROJECT_NAME, **location_parameters)

                storage_client = GoogleCloudStorageClient(TEST_PROJECT_NAME)

                persisted_file_0 = storage_client.download_as_string(
                    cloud_path=storage.path.join(cloud_path, dataset.name, "file_0.txt"),
                )

                self.assertEqual(persisted_file_0, "[1, 2, 3]")

                persisted_file_1 = storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(output_directory, dataset.name, "file_1.txt"),
                )
                self.assertEqual(persisted_file_1, "[4, 5, 6]")

                persisted_dataset = json.loads(
                    storage_client.download_as_string(
                        bucket_name=TEST_BUCKET_NAME,
                        path_in_bucket=storage.path.join(
                            output_directory, dataset.name, definitions.DATASET_METADATA_FILENAME
                        ),
                    )
                )

                self.assertEqual(
                    persisted_dataset["files"],
                    [
                        "gs://octue-test-bucket/my_datasets/octue-sdk-python/file_0.txt",
                        "gs://octue-test-bucket/my_datasets/octue-sdk-python/file_1.txt",
                    ],
                )

                self.assertEqual(persisted_dataset["tags"], dataset.tags.to_primitive())

    def test_download_all_files(self):
        """Test that all files in a dataset can be downloaded with one command."""
        storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

        dataset_name = "another-dataset"
        storage_client.upload_from_string(
            string=json.dumps([1, 2, 3]), bucket_name=TEST_BUCKET_NAME, path_in_bucket=f"{dataset_name}/file_0.txt"
        )
        storage_client.upload_from_string(
            string=json.dumps([4, 5, 6]), bucket_name=TEST_BUCKET_NAME, path_in_bucket=f"{dataset_name}/file_1.txt"
        )

        dataset = Dataset.from_cloud(
            project_name=TEST_PROJECT_NAME, cloud_path=f"gs://{TEST_BUCKET_NAME}/{dataset_name}"
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset.download_all_files(local_directory=temporary_directory)

            with open(os.path.join(temporary_directory, "file_0.txt")) as f:
                self.assertEqual(f.read(), "[1, 2, 3]")

            with open(os.path.join(temporary_directory, "file_1.txt")) as f:
                self.assertEqual(f.read(), "[4, 5, 6]")

    def test_download_all_files_from_nested_dataset(self):
        """Test that all files in a nested dataset can be downloaded with one command."""
        self._create_nested_cloud_dataset("nested_dataset")

        dataset = Dataset.from_cloud(
            project_name=TEST_PROJECT_NAME,
            cloud_path=f"gs://{TEST_BUCKET_NAME}/nested_dataset",
            recursive=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset.download_all_files(local_directory=temporary_directory)

            with open(os.path.join(temporary_directory, "file_0.txt")) as f:
                self.assertEqual(f.read(), "[1, 2, 3]")

            with open(os.path.join(temporary_directory, "file_1.txt")) as f:
                self.assertEqual(f.read(), "[4, 5, 6]")

            with open(os.path.join(temporary_directory, "sub-directory", "sub_file.txt")) as f:
                self.assertEqual(f.read(), "['a', 'b', 'c']")

            with open(os.path.join(temporary_directory, "sub-directory", "sub-sub-directory", "sub_sub_file.txt")) as f:
                self.assertEqual(f.read(), "['blah', 'b', 'c']")
