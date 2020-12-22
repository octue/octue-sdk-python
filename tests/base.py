import os
import subprocess
import unittest
import uuid
from tempfile import TemporaryDirectory, gettempdir

from octue.mixins import MixinBase, Pathable
from octue.resources import Datafile, Dataset


class MyPathable(Pathable, MixinBase):
    pass


class BaseTestCase(unittest.TestCase):
    """ Base test case for twined:
        - sets a path to the test data directory
    """

    def setUp(self):

        # Set up paths to the test data directory and to the app templates directory
        root_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_path = os.path.join(root_dir, "data")
        self.templates_path = os.path.join(os.path.dirname(root_dir), "octue", "templates")
        super().setUp()

    def callCli(self, args):
        """ Utility to call the octue CLI (eg for a templated example) in a separate subprocess
        Enables testing that multiple processes aren't using the same memory space, or for running multiple apps in
        parallel to ensure they don't conflict
        """
        call_id = str(uuid.uuid4())
        tmp_dir_name = os.path.join(gettempdir(), "octue-sdk-python", f"test-{call_id}")

        with TemporaryDirectory(dir=tmp_dir_name):
            subprocess.call(args, cwd=tmp_dir_name)

    def create_valid_dataset(self):
        path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        path = os.path.join("path-within-dataset", "a_test_file.csv")

        files = [
            Datafile(path_from=path_from, base_from=path_from, path=path, skip_checks=False),
            Datafile(path_from=path_from, base_from=path_from, path=path, skip_checks=False),
        ]

        return Dataset(files=files)
