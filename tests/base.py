import os
import unittest

import yaml

from octue.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier
from octue.mixins import MixinBase, Pathable
from octue.resources import Datafile, Dataset, Manifest
from tests import TEST_BUCKET_NAME


class MyPathable(Pathable, MixinBase):
    pass


class BaseTestCase(unittest.TestCase):
    """Base test case for twined:
    - sets a path to the test data directory
    """

    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    def setUp(self):
        """Set up the test case by:
        - Adding the paths to the test data and app templates directories to the test case
        - Making `unittest` ignore excess ResourceWarnings so tests' console outputs are clearer. This has to be done
        even if these warnings are ignored elsewhere as unittest forces warnings to be displayed by default.

        :return None:
        """
        root_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_path = os.path.join(root_dir, "data")
        self.templates_path = os.path.join(os.path.dirname(root_dir), "octue", "templates")

        super().setUp()

    def create_valid_dataset(self, **kwargs):
        """Create a valid dataset with two valid datafiles (they're the same file in this case)."""
        path = os.path.join(self.data_path, "basic_files", "configuration", "test-dataset")

        return Dataset(
            path=path,
            files=[
                Datafile(path=os.path.join(path, "path-within-dataset", "a_test_file.csv")),
                Datafile(path=os.path.join(path, "path-within-dataset", "another_test_file.csv")),
            ],
            **kwargs
        )

    def create_valid_manifest(self):
        """Create a valid manifest with two valid datasets (they're the same dataset in this case)."""
        datasets = {"my_dataset": self.create_valid_dataset(), "another_dataset": self.create_valid_dataset()}
        manifest = Manifest(datasets=datasets)
        return manifest

    def _create_octue_configuration_file(self, octue_configuration, directory_path):
        """Create an `octue.yaml` configuration file in the given directory.

        :param str directory_path:
        :return str: the path of the `octue.yaml` file
        """
        octue_configuration_path = os.path.join(directory_path, "octue.yaml")

        with open(octue_configuration_path, "w") as f:
            yaml.dump(octue_configuration, f)

        return octue_configuration_path
