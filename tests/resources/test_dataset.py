import copy
import json
import logging
import os
import tempfile
from unittest.mock import patch

from octue import REPOSITORY_ROOT, exceptions
from octue.cloud import storage
from octue.cloud.emulators.cloud_storage import mock_generate_signed_url
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Datafile, Dataset
from octue.resources.dataset import SIGNED_METADATA_DIRECTORY
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase
from tests.resources import create_dataset_with_two_files


class TestDataset(BaseTestCase):
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

    def test_empty_dataset_logs_warning(self):
        """Test that datasets that are empty at instantiation time log a warning."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertLogs(level=logging.WARNING) as logging_context:
                Dataset(temporary_directory)

        self.assertIn(f"is empty at instantiation time (path {temporary_directory!r}).", logging_context.output[0])

    def test_iter(self):
        """Test that iterating over a Dataset is equivalent to iterating over its files."""
        dataset = self.create_valid_dataset()
        iterated_files = {file for file in dataset}
        self.assertEqual(iterated_files, dataset.files)

    def test_error_raised_if_files_invalid(self):
        """Test that an error is raised if the `files` instantiation parameter is invalid."""
        for invalid_files in ("some/path", {"some": "value"}):
            with self.subTest(files=invalid_files):
                with self.assertRaises(exceptions.InvalidInputException):
                    Dataset(files=invalid_files)

    def test_cannot_add_non_datafiles(self):
        """Ensures that exception will be raised if adding a non-datafile object"""

        class NotADatafile:
            pass

        resource = Dataset()
        with self.assertRaises(exceptions.InvalidInputException):
            resource.add(NotADatafile())

    def test_adding_cloud_datafile_to_cloud_dataset(self):
        """Test that a cloud datafile can be added to a cloud dataset and that it's copied into the dataset root if no
        `path_within_dataset` is provided.
        """
        dataset = Dataset(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "dataset"))

        with Datafile(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "datafile.dat"), mode="w") as (
            datafile,
            f,
        ):
            f.write("hello")

        dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.cloud_path, storage.path.join(dataset.path, "datafile.dat"))

    def test_adding_cloud_datafile_to_cloud_dataset_when_file_is_already_in_dataset_directory(self):
        """Test that a cloud datafile's path is kept as-is when adding it to a cloud dataset if it is already in the
        dataset directory and no `path_in_dataset` is provided.
        """
        dataset = Dataset(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "dataset"))

        with Datafile(path=storage.path.join(dataset.path, "subfolder", "datafile.dat"), mode="w") as (datafile, f):
            f.write("hello")

        dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.cloud_path, storage.path.join(dataset.path, "subfolder", "datafile.dat"))

    def test_providing_path_when_adding_cloud_datafile_to_cloud_dataset_copies_datafile_to_path(self):
        """Test that providing the `path_within_dataset` parameter when adding a cloud datafile to a cloud dataset
        results in the datafile being copied to that location within the dataset.
        """
        dataset = Dataset(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "dataset"))

        with Datafile(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "datafile.dat"), mode="w") as (
            datafile,
            f,
        ):
            f.write("hello")

        path_in_dataset = storage.path.join("another", "path", "datafile.dat")
        dataset.add(datafile, path_in_dataset=path_in_dataset)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.cloud_path, storage.path.join(dataset.path, path_in_dataset))

    def test_adding_local_datafile_to_cloud_dataset_uploads_it_to_dataset_root(self):
        """Test that, when adding a local datafile to a cloud dataset and `path_in_dataset` is not provided, the
        datafile is uploaded to the root of the dataset.
        """
        dataset = Dataset(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "dataset"))

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Datafile(path=os.path.join(temporary_directory, "path", "to", "datafile.dat"), mode="w") as (
                datafile,
                f,
            ):
                f.write("hello")

            dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.cloud_path, storage.path.join(dataset.path, "datafile.dat"))

    def test_providing_path_when_adding_local_datafile_to_cloud_dataset(self):
        """Test that, when adding a local datafile to a cloud dataset and providing the `path_within_dataset` parameter,
        the datafile is uploaded to that path within the dataset.
        """
        dataset = Dataset(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "dataset"))

        with tempfile.TemporaryDirectory() as temporary_directory:
            with Datafile(path=os.path.join(temporary_directory, "path", "to", "datafile.dat"), mode="w") as (
                datafile,
                f,
            ):
                f.write("hello")

            path_in_dataset = storage.path.join("another", "path", "datafile.dat")
            dataset.add(datafile, path_in_dataset=path_in_dataset)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.cloud_path, storage.path.join(dataset.path, path_in_dataset))

    def test_adding_local_datafile_to_local_dataset(self):
        """Test that a local datafile can be added to a local dataset and that it is copied to the root of the dataset
        if no `path_within_dataset` is provided.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=os.path.join(temporary_directory, "path", "to", "dataset"))

            with Datafile(path=os.path.join(temporary_directory, "path", "to", "datafile.dat"), mode="w") as (
                datafile,
                f,
            ):
                f.write("hello")

            dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.local_path, os.path.join(dataset.path, "datafile.dat"))

    def test_adding_local_datafile_to_local_dataset_when_file_is_already_in_dataset_directory(self):
        """Test that a local datafile's path is kept as-is when adding it to a local dataset if it is already in the
        dataset directory.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=os.path.join(temporary_directory, "path", "to", "dataset"))

            with Datafile(path=os.path.join(dataset.path, "subfolder", "datafile.dat"), mode="w") as (datafile, f):
                f.write("hello")

            dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.local_path, os.path.join(dataset.path, "subfolder", "datafile.dat"))

    def test_providing_path_when_adding_local_datafile_to_local_dataset(self):
        """Test that providing the `path_within_dataset` parameter when adding a local datafile to a local dataset
        results in the datafile being copied to that location within the dataset.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=os.path.join(temporary_directory, "path", "to", "dataset"))

            with Datafile(path=os.path.join(temporary_directory, "path", "to", "datafile.dat"), mode="w") as (
                datafile,
                f,
            ):
                f.write("hello")

            path_in_dataset = os.path.join("another", "path", "datafile.dat")
            dataset.add(datafile, path_in_dataset=path_in_dataset)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.local_path, os.path.join(dataset.path, path_in_dataset))

    def test_adding_cloud_datafile_to_local_dataset(self):
        """Test that when a cloud datafile is added to a local dataset, it is downloaded to the root of the dataset."""
        with Datafile(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "datafile.dat"), mode="w") as (
            datafile,
            f,
        ):
            f.write("hello")

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=os.path.join(temporary_directory, "path", "to", "dataset"))
            dataset.add(datafile)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.local_path, os.path.join(dataset.path, "datafile.dat"))

    def test_providing_path_in_dataset_when_adding_cloud_datafile_to_local_dataset(self):
        """Test that when a cloud datafile is added to a local dataset and the `path_in_dataset` parameter is provided,
        it is downloaded to that path within the dataset.
        """
        with Datafile(path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "path", "to", "datafile.dat"), mode="w") as (
            datafile,
            f,
        ):
            f.write("hello")

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=os.path.join(temporary_directory, "path", "to", "dataset"))

            path_in_dataset = os.path.join("another", "path", "datafile.dat")
            dataset.add(datafile, path_in_dataset=path_in_dataset)

        self.assertIn(datafile, dataset)
        self.assertEqual(datafile.local_path, os.path.join(dataset.path, path_in_dataset))

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
        self.assertEqual(resource.get_file_by_label("three").labels, files[2].labels)

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

        self.assertEqual(
            Dataset(files=files).files.filter(name__icontains="txt").pop().local_path,
            files[1].local_path,
        )

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

    def test_metadata_hash_is_same_for_different_datasets_with_the_same_metadata(self):
        """Test that the metadata hash is the same for datasets with different files but the same metadata."""
        first_dataset = Dataset(labels={"a", "b", "c"})
        second_dataset = Dataset(files={Datafile(path="blah", ignore_stored_metadata=True)}, labels={"a", "b", "c"})
        self.assertEqual(first_dataset.metadata_hash_value, second_dataset.metadata_hash_value)

    def test_metadata_hash_is_different_for_same_dataset_but_different_metadata(self):
        """Test that the metadata hash is different for datasets with the same files but different metadata."""
        first_dataset = self.create_valid_dataset()
        second_dataset = copy.deepcopy(first_dataset)
        second_dataset.labels = {"d", "e", "f"}
        self.assertNotEqual(first_dataset.metadata_hash_value, second_dataset.metadata_hash_value)

    def test_serialisation_and_deserialisation(self):
        """Test that a dataset can be serialised and deserialised."""
        dataset_id = "e376fb31-8f66-414d-b99f-b43395cebbf1"
        dataset = self.create_valid_dataset(id=dataset_id, labels=["b", "a"], tags={"a": 1, "b": 2})

        serialised_dataset = dataset.to_primitive()

        self.assertEqual(
            serialised_dataset,
            {
                "name": "test-dataset",
                "labels": ["a", "b"],
                "tags": {"a": 1, "b": 2},
                "id": dataset_id,
                "path": os.path.join(REPOSITORY_ROOT, "tests", "data", "basic_files", "configuration", "test-dataset"),
                "files": [
                    os.path.join(
                        REPOSITORY_ROOT,
                        "tests",
                        "data",
                        "basic_files",
                        "configuration",
                        "test-dataset",
                        "path-within-dataset",
                        "a_test_file.csv",
                    ),
                    os.path.join(
                        REPOSITORY_ROOT,
                        "tests",
                        "data",
                        "basic_files",
                        "configuration",
                        "test-dataset",
                        "path-within-dataset",
                        "another_test_file.csv",
                    ),
                ],
            },
        )

        deserialised_dataset = Dataset.deserialise(serialised_dataset)
        self.assertEqual(dataset.id, deserialised_dataset.id)
        self.assertEqual(dataset.path, deserialised_dataset.path)
        self.assertEqual(dataset.name, deserialised_dataset.name)
        self.assertEqual(dataset.labels, deserialised_dataset.labels)
        self.assertEqual(dataset.tags, deserialised_dataset.tags)

    def test_exists_in_cloud(self):
        """Test whether all files of a dataset are in the cloud or not can be determined."""
        self.assertFalse(self.create_valid_dataset().all_files_are_in_cloud)

        with tempfile.TemporaryDirectory() as temporary_directory:
            self.assertTrue(Dataset(path=temporary_directory).all_files_are_in_cloud)

        files = [Datafile(path="gs://hello/file.txt"), Datafile(path="gs://goodbye/file.csv")]
        self.assertTrue(Dataset(files=files).all_files_are_in_cloud)

    def test_from_cloud(self):
        """Test that a Dataset in cloud storage can be accessed via a cloud path."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset.tags = {"a": "b", "c": 1}

            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "a_directory", dataset.name)
            dataset.upload(cloud_path)
            persisted_dataset = Dataset(path=cloud_path)

            self.assertEqual(persisted_dataset.path, f"gs://{TEST_BUCKET_NAME}/a_directory/{dataset.name}")
            self.assertEqual(persisted_dataset.id, dataset.id)
            self.assertEqual(persisted_dataset.name, dataset.name)
            self.assertEqual(persisted_dataset.hash_value, dataset.hash_value)
            self.assertEqual(persisted_dataset.tags, dataset.tags)
            self.assertEqual(persisted_dataset.labels, dataset.labels)
            self.assertEqual({file.name for file in persisted_dataset.files}, {file.name for file in dataset.files})

            for file in persisted_dataset:
                self.assertEqual(file.cloud_path, f"gs://{TEST_BUCKET_NAME}/a_directory/{dataset.name}/{file.name}")

    def test_from_cloud_with_no_metadata_file(self):
        """Test that any cloud directory can be accessed as a dataset if it has no `.octue` metadata file in it, the
        cloud dataset doesn't lose any information during serialization, and a metadata file is uploaded afterwards.
        """
        cloud_storage_client = GoogleCloudStorageClient()

        cloud_storage_client.upload_from_string(
            "[1, 2, 3]",
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset", "file_0.txt"),
        )

        cloud_storage_client.upload_from_string(
            "[4, 5, 6]",
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_dataset", "file_1.txt"),
        )

        cloud_dataset = Dataset(path=f"gs://{TEST_BUCKET_NAME}/my_dataset")

        self.assertEqual(cloud_dataset.path, f"gs://{TEST_BUCKET_NAME}/my_dataset")
        self.assertEqual(cloud_dataset.name, "my_dataset")
        self.assertEqual({file.name for file in cloud_dataset.files}, {"file_0.txt", "file_1.txt"})

        for file in cloud_dataset:
            self.assertEqual(file.cloud_path, f"gs://{TEST_BUCKET_NAME}/my_dataset/{file.name}")

        # Test serialisation doesn't lose any information.
        deserialised_dataset = Dataset.deserialise(cloud_dataset.to_primitive())
        self.assertEqual(deserialised_dataset.id, cloud_dataset.id)
        self.assertEqual(deserialised_dataset.name, cloud_dataset.name)
        self.assertEqual(deserialised_dataset.path, cloud_dataset.path)
        self.assertEqual(deserialised_dataset.hash_value, cloud_dataset.hash_value)

    def test_from_cloud_with_nested_dataset_and_no_metadata_file(self):
        """Test that a nested dataset is loaded from the cloud correctly if it has no `.octue` metadata file in it."""
        dataset_path = self.create_nested_cloud_dataset(dataset_name="nested_dataset_with_no_metadata")

        cloud_dataset = Dataset(path=dataset_path)

        self.assertEqual(cloud_dataset.path, dataset_path)
        self.assertEqual(cloud_dataset.name, "nested_dataset_with_no_metadata")

        self.assertEqual(
            {file.name for file in cloud_dataset.files},
            {"file_0.txt", "file_1.txt", "sub_file.txt", "sub_sub_file.txt"},
        )

    def test_update_local_metadata(self):
        """Test that metadata for a local dataset can be stored locally and used on re-instantiation of the same
        dataset.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            self._create_files_and_nested_subdirectories(temporary_directory)

            dataset = Dataset(
                path=temporary_directory,
                id="69253db4-7972-42de-8ccc-61336a28cd50",
                tags={"cat": "dog"},
                labels=["animals"],
            )

            dataset.update_local_metadata()

            dataset_reloaded = Dataset(path=temporary_directory)
            self.assertEqual(dataset.id, dataset_reloaded.id)
            self.assertEqual(dataset.tags, dataset_reloaded.tags)
            self.assertEqual(dataset.labels, dataset_reloaded.labels)
            self.assertEqual(dataset.hash_value, dataset_reloaded.hash_value)

    def test_update_cloud_metadata(self):
        """Test that metadata for a cloud dataset can be stored in the cloud and used on re-instantiation of the same
        dataset.
        """
        dataset_path = self.create_nested_cloud_dataset()
        dataset = Dataset(path=dataset_path)
        self.assertEqual(dataset.tags, {})

        dataset.tags = {"some": "tags"}
        dataset.update_cloud_metadata()

        dataset_reloaded = Dataset(path=dataset_path)
        self.assertEqual(dataset.id, dataset_reloaded.id)
        self.assertEqual(dataset.tags, dataset_reloaded.tags)
        self.assertEqual(dataset.labels, dataset_reloaded.labels)
        self.assertEqual(dataset.hash_value, dataset_reloaded.hash_value)

    def test_upload(self):
        """Test that a dataset can be uploaded to a cloud path, including all its files and the dataset's metadata."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = create_dataset_with_two_files(temporary_directory)
            dataset.tags = {"a": "b", "c": 1}

            output_directory = "my_datasets"
            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, output_directory, dataset.name)
            dataset.upload(cloud_path)

            storage_client = GoogleCloudStorageClient()

            # Check its files have been uploaded.
            persisted_file_0 = storage_client.download_as_string(storage.path.join(cloud_path, "file_0.txt"))
            self.assertEqual(persisted_file_0, "0")

            persisted_file_1 = storage_client.download_as_string(storage.path.join(cloud_path, "file_1.txt"))
            self.assertEqual(persisted_file_1, "1")

            # Check its metadata has been uploaded.
            dataset._get_cloud_metadata()
            self.assertEqual(dataset._cloud_metadata["tags"], dataset.tags.to_primitive())

    def test_upload_with_nested_dataset_preserves_nested_structure(self):
        """Test that uploading a dataset containing datafiles in a nested directory structure to the cloud preserves
        this structure in the cloud.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            local_paths = self._create_files_and_nested_subdirectories(temporary_directory)
            dataset = Dataset(path=temporary_directory)

            upload_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-dataset")
            dataset.upload(cloud_path=upload_path)

        cloud_datafile_relative_paths = {
            blob.name.split(dataset.name)[-1].strip("/")
            for blob in GoogleCloudStorageClient().scandir(
                upload_path,
                filter=lambda blob: not blob.name.endswith(".octue") and SIGNED_METADATA_DIRECTORY not in blob.name,
            )
        }

        # Check that the paths relative to the dataset directory are the same in the cloud as they are locally.
        local_datafile_relative_paths = {
            path.split(temporary_directory)[-1].strip(os.path.sep).replace(os.path.sep, "/") for path in local_paths
        }

        self.assertEqual(cloud_datafile_relative_paths, local_datafile_relative_paths)

    def test_upload_works_with_implicit_cloud_location_if_cloud_location_previously_provided(self):
        """Test `Dataset.to_cloud` works with an implicit cloud location if the cloud location has previously been
        provided.
        """
        dataset_path = self.create_nested_cloud_dataset()
        dataset = Dataset(path=dataset_path)
        dataset.upload()

    def test_upload_to_new_location(self):
        """Test that a dataset can be uploaded to a new cloud location."""
        dataset_path = self.create_nested_cloud_dataset()
        dataset = Dataset(dataset_path, recursive=False)

        new_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "new", "dataset", "location")
        dataset.upload(new_cloud_path)

        self.assertEqual(dataset.path, new_cloud_path)

        self.assertEqual(
            {datafile.cloud_path for datafile in dataset.files},
            {storage.path.join(new_cloud_path, "file_0.txt"), storage.path.join(new_cloud_path, "file_1.txt")},
        )

    def test_error_raised_if_trying_to_download_files_from_local_dataset(self):
        """Test that an error is raised if trying to download files from a local dataset."""
        dataset = self.create_valid_dataset()

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            dataset.download()

    def test_download(self):
        """Test that all files in a dataset can be downloaded with one command."""
        storage_client = GoogleCloudStorageClient()

        dataset_name = "another-dataset"
        storage_client.upload_from_string(
            string=json.dumps([1, 2, 3]),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, dataset_name, "file_0.txt"),
        )
        storage_client.upload_from_string(
            string=json.dumps([4, 5, 6]),
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, dataset_name, "file_1.txt"),
        )

        dataset = Dataset(path=f"gs://{TEST_BUCKET_NAME}/{dataset_name}")

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset.download(local_directory=temporary_directory)

            with open(os.path.join(temporary_directory, "file_0.txt")) as f:
                self.assertEqual(f.read(), "[1, 2, 3]")

            with open(os.path.join(temporary_directory, "file_1.txt")) as f:
                self.assertEqual(f.read(), "[4, 5, 6]")

    def test_download_from_nested_dataset(self):
        """Test that all files in a nested dataset can be downloaded with one command."""
        dataset_path = self.create_nested_cloud_dataset()

        dataset = Dataset(path=dataset_path)

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset.download(local_directory=temporary_directory)

            with open(os.path.join(temporary_directory, "file_0.txt")) as f:
                self.assertEqual(f.read(), "[1, 2, 3]")

            with open(os.path.join(temporary_directory, "file_1.txt")) as f:
                self.assertEqual(f.read(), "[4, 5, 6]")

            with open(os.path.join(temporary_directory, "sub-directory", "sub_file.txt")) as f:
                self.assertEqual(f.read(), "['a', 'b', 'c']")

            with open(os.path.join(temporary_directory, "sub-directory", "sub-sub-directory", "sub_sub_file.txt")) as f:
                self.assertEqual(f.read(), "['blah', 'b', 'c']")

    def test_download_from_nested_dataset_with_no_local_directory_given(self):
        """Test that, when downloading all files from a nested dataset and no local directory is given, the dataset
        structure is preserved in the temporary directory used.
        """
        dataset_path = self.create_nested_cloud_dataset()

        dataset = Dataset(path=dataset_path)

        # Mock the temporary directory created in `Dataset.download_all_files` so we can access it for the test.
        temporary_directory = tempfile.TemporaryDirectory()

        with patch("octue.resources.dataset.RegisteredTemporaryDirectory", return_value=temporary_directory):
            dataset.download()

        with open(os.path.join(temporary_directory.name, "file_0.txt")) as f:
            self.assertEqual(f.read(), "[1, 2, 3]")

        with open(os.path.join(temporary_directory.name, "file_1.txt")) as f:
            self.assertEqual(f.read(), "[4, 5, 6]")

        with open(os.path.join(temporary_directory.name, "sub-directory", "sub_file.txt")) as f:
            self.assertEqual(f.read(), "['a', 'b', 'c']")

        with open(
            os.path.join(temporary_directory.name, "sub-directory", "sub-sub-directory", "sub_sub_file.txt")
        ) as f:
            self.assertEqual(f.read(), "['blah', 'b', 'c']")

    def test_from_local_directory(self):
        """Test that a dataset can be instantiated from a local nested directory ignoring its subdirectories and that
        extra keyword arguments can be provided for the dataset instantiation.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            paths = self._create_files_and_nested_subdirectories(temporary_directory)
            dataset = Dataset(path=temporary_directory, recursive=False, name="my-dataset")
            self.assertEqual(dataset.name, "my-dataset")

            # Check that just the top-level files from the directory are present in the dataset.
            datafile_paths = {datafile.local_path for datafile in dataset.files}
            self.assertEqual(datafile_paths, set(paths[:2]))

    def test_from_local_directory_recursively(self):
        """Test that a dataset can be instantiated from a local nested directory including its subdirectories."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            paths = self._create_files_and_nested_subdirectories(temporary_directory)
            dataset = Dataset(path=temporary_directory)

            # Check that all the files from the directory are present in the dataset.
            datafile_paths = {datafile.local_path for datafile in dataset.files}
            self.assertEqual(datafile_paths, set(paths))

    def test_error_raised_if_attempting_to_generate_signed_url_for_local_dataset(self):
        """Test that an error is raised if trying to generate a signed URL for a local dataset."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=temporary_directory, tags={"hello": "world"})

            with self.assertRaises(exceptions.CloudLocationNotSpecified):
                dataset.generate_signed_url()

    def test_generating_signed_url_from_dataset_and_recreating_dataset_from_it(self):
        """Test that a signed URL can be generated for a dataset that can be used to recreate/get it, its metadata, and
        all its files.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_local_path = os.path.join(temporary_directory, "my-dataset-to-sign")

            with Datafile(path=os.path.join(dataset_local_path, "my-file.dat"), mode="w") as (datafile, f):
                f.write("hello")
                datafile.tags = {"my": "metadata"}

            dataset = Dataset(path=dataset_local_path, tags={"hello": "world"})
            dataset.upload(storage.path.generate_gs_path(TEST_BUCKET_NAME, "my-dataset-to-sign"))

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
            signed_url = dataset.generate_signed_url()

        downloaded_dataset = Dataset(path=signed_url)
        self.assertEqual(downloaded_dataset.tags, {"hello": "world"})

        with downloaded_dataset.files.one() as (downloaded_datafile, f):
            self.assertEqual(f.read(), "hello")

        self.assertEqual(downloaded_datafile.name, "my-file.dat")
        self.assertEqual(downloaded_datafile.extension, "dat")

        # Check that the datafile's cloud metadata is retrieved. This assertion will be uncommented when this issue
        # https://github.com/oittaa/gcp-storage-emulator/issues/187 with the storage emulator is resolved. See issue
        # https://github.com/octue/octue-sdk-python/issues/489.

        # self.assertEqual(downloaded_datafile.tags, {"my": "metadata"})

    def test_error_logged_when_instantiating_dataset_from_inaccessible_url(self):
        """Test that an error is logged but not raised when attempting to instantiate a dataset from an inaccessible
        URL.
        """
        with self.assertLogs(level=logging.ERROR) as logging_context:
            dataset = Dataset("https://non.existent/dataset")

        self.assertIn(
            "Couldn't access cloud dataset metadata for 'https://non.existent/dataset'; proceeding without cloud "
            "metadata.",
            logging_context.output[0],
        )

        self.assertEqual(dataset.path, "https://non.existent/dataset")

    def test_exiting_context_manager_of_local_dataset_updates_local_metadata(self):
        """Test that local metadata for a local dataset is updated on exit of the dataset context manager."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            self._create_files_and_nested_subdirectories(temporary_directory)

            dataset = Dataset(path=temporary_directory)

            with dataset:
                dataset.tags = {"cat": "dog"}
                dataset.labels = {"animals"}

            reloaded_dataset = Dataset(path=temporary_directory)
            self.assertEqual(reloaded_dataset.id, dataset.id)
            self.assertEqual(reloaded_dataset.tags, {"cat": "dog"})
            self.assertEqual(reloaded_dataset.labels, {"animals"})

    def test_exiting_context_manager_of_cloud_dataset_updates_cloud_metadata(self):
        """Test that cloud metadata for a cloud dataset is updated on exit of the dataset context manager."""
        dataset_path = self.create_nested_cloud_dataset()
        dataset = Dataset(path=dataset_path)

        with dataset:
            dataset.tags = {"cat": "dog"}
            dataset.labels = {"animals"}

        reloaded_dataset = Dataset(path=dataset_path)
        self.assertEqual(reloaded_dataset.id, dataset.id)
        self.assertEqual(reloaded_dataset.tags, {"cat": "dog"})
        self.assertEqual(reloaded_dataset.labels, {"animals"})

    def test_stored_metadata_has_priority_over_instantiation_metadata_if_not_ignoring_stored_metadata(self):
        """Test that stored metadata is used instead of instantiation metadata if `ignore_stored_metadata` is `False`."""
        cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "existing_dataset")

        # Create a dataset in the cloud and set some metadata on it.
        with Dataset(path=cloud_path) as dataset:
            dataset.tags = {"existing": True}

        # Load it separately from the cloud object and check that the stored metadata is used instead of the
        # instantiation metadata.
        reloaded_dataset = Dataset(path=cloud_path, tags={"new": "tag"})
        self.assertEqual(reloaded_dataset.tags, {"existing": True})

    def test_instantiation_metadata_used_if_not_ignoring_stored_metadata_but_no_stored_metadata(self):
        """Test that instantiation metadata is used if `ignore_stored_metadata` is `False` but there's no stored metadata."""
        cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "non_existing_dataset")
        dataset = Dataset(path=cloud_path, tags={"new": "tag"})
        self.assertEqual(dataset.tags, {"new": "tag"})

    def test_stored_metadata_ignored_if_ignoring_stored_metadata(self):
        """Test that instantiation metadata is used instead of stored metadata if `ignore_stored_metadata` is `True`."""
        cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "existing_dataset")

        # Create a dataset in the cloud and set some metadata on it.
        with Dataset(path=cloud_path) as dataset:
            dataset.tags = {"existing": True}

        # Load it separately from the cloud object and check that the instantiation metadata is used instead of the
        # stored metadata.
        reloaded_datafile = Dataset(path=cloud_path, tags={"new": "tag"}, ignore_stored_metadata=True)
        self.assertEqual(reloaded_datafile.tags, {"new": "tag"})

    def test_update_metadata_with_local_dataset(self):
        """Test the `update_metadata` method with a local dataset."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset = Dataset(path=temporary_directory)

            # Update the instance metadata but don't update the local stored metadata.
            dataset.tags["hello"] = "world"

            # Check the instance metadata hasn't been stored locally.
            self.assertEqual(Dataset(path=temporary_directory).tags, {})

            # Update the local stored metadata and check it.
            dataset.update_metadata()
            self.assertEqual(Dataset(path=temporary_directory).tags, {"hello": "world"})

    def test_update_metadata_with_cloud_dataset(self):
        """Test the `update_metadata` method with a cloud dataset."""
        dataset_path = self.create_nested_cloud_dataset()
        dataset = Dataset(path=dataset_path)

        # Update the instance metadata but don't update the cloud stored metadata.
        dataset.tags["hello"] = "world"

        # Check the instance metadata hasn't been stored in the cloud.
        self.assertEqual(Dataset(path=dataset.path).tags, {})

        # Update the cloud stored metadata and check it.
        dataset.update_metadata()
        self.assertEqual(Dataset(path=dataset.path).tags, {"hello": "world"})

    def test_name_of_dataset_with_trailing_slash_is_correct(self):
        """Test that the name of a dataset with a trailing slash is correct."""
        cloud_path = self.create_nested_cloud_dataset("my-dataset")
        dataset = Dataset(cloud_path + "/")
        self.assertEqual(dataset.name, "my-dataset")

    def _create_files_and_nested_subdirectories(self, directory_path):
        """Create files and nested subdirectories of files in the given directory.

        :param str directory_path: the directory to create the nested structure in
        :return list(str): the paths of the files in the directory and subdirectories
        """
        paths = [
            os.path.join(directory_path, "file_0.txt"),
            os.path.join(directory_path, "file_1.txt"),
            os.path.join(directory_path, "sub-directory", "sub_file.txt"),
            os.path.join(directory_path, "sub-directory", "sub-sub-directory", "sub_sub_file.txt"),
        ]

        os.makedirs(os.path.join(directory_path, "sub-directory", "sub-sub-directory"))

        # Create nested files in directory.
        for path, data in zip(paths, range(len(paths))):
            with open(path, "w") as f:
                f.write(str(data))

        return paths
