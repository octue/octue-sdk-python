import os
import unittest

import coolname

from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.definitions import DATA_PATH
from octue.resources import Datafile, Dataset, Manifest
from tests import TEST_BUCKET_NAME
from tests.twined.base import TestResultModifier


class BaseTestCase(unittest.TestCase):
    """Base test case for twined:
    - sets a path to the test data directory
    """

    test_result_modifier = TestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    def create_valid_dataset(self, **kwargs):
        """Create a valid dataset with two valid datafiles (they're the same file in this case)."""
        path = os.path.join(DATA_PATH, "basic_files", "configuration", "test-dataset")

        return Dataset(
            path=path,
            files=[
                Datafile(path=os.path.join(path, "path-within-dataset", "a_test_file.csv")),
                Datafile(path=os.path.join(path, "path-within-dataset", "another_test_file.csv")),
            ],
            **kwargs,
        )

    def create_valid_manifest(self):
        """Create a valid manifest with two valid datasets (they're the same dataset in this case)."""
        datasets = {"my_dataset": self.create_valid_dataset(), "another_dataset": self.create_valid_dataset()}
        manifest = Manifest(datasets=datasets)
        return manifest

    def create_nested_cloud_dataset(self, dataset_name=None):
        """Create a dataset in cloud storage with the given name containing a nested set of files.

        :param str|None dataset_name: the name to give the dataset; a random name is generated if none is given
        :return str: the cloud path for the dataset
        """
        cloud_storage_client = GoogleCloudStorageClient()
        dataset_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, dataset_name or coolname.generate_slug(2))

        cloud_storage_client.upload_from_string("[1, 2, 3]", cloud_path=storage.path.join(dataset_path, "file_0.txt"))
        cloud_storage_client.upload_from_string("[4, 5, 6]", cloud_path=storage.path.join(dataset_path, "file_1.txt"))

        cloud_storage_client.upload_from_string(
            "['a', 'b', 'c']",
            cloud_path=storage.path.join(dataset_path, "sub-directory", "sub_file.txt"),
        )

        cloud_storage_client.upload_from_string(
            "['blah', 'b', 'c']",
            cloud_path=storage.path.join(dataset_path, "sub-directory", "sub-sub-directory", "sub_sub_file.txt"),
        )

        return dataset_path
