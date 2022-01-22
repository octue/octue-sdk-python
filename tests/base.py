import os
import unittest

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

    def create_valid_dataset(self):
        """Create a valid dataset with two valid datafiles (they're the same file in this case)."""
        path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        path = os.path.join("path-within-dataset", "a_test_file.csv")

        files = [
            Datafile(path_from=path_from, path=path, skip_checks=False),
            Datafile(path_from=path_from, path=path, skip_checks=False),
        ]

        return Dataset(files=files)

    def create_valid_manifest(self):
        """Create a valid manifest with two valid datasets (they're the same dataset in this case)."""
        datasets = [self.create_valid_dataset(), self.create_valid_dataset()]
        manifest = Manifest(datasets=datasets, keys={"my_dataset": 0, "another_dataset": 1})
        return manifest
