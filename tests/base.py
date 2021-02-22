import logging
import os
import subprocess
import unittest
import uuid
from tempfile import TemporaryDirectory, gettempdir
from gcloud_storage_emulator.server import create_server
from google.cloud import storage

from octue.logging_handlers import apply_log_handler
from octue.mixins import MixinBase, Pathable
from octue.resources import Datafile, Dataset, Manifest
from octue.resources.communication import Service
from octue.utils.cloud.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)
apply_log_handler(logger, log_level=logging.DEBUG)


class MyPathable(Pathable, MixinBase):
    pass


class BaseTestCase(unittest.TestCase):
    """Base test case for twined:
    - sets a path to the test data directory
    """

    storage_emulator = create_server("localhost", 9090, in_memory=True)
    project_name = os.environ["TEST_PROJECT_NAME"]
    bucket_name = os.environ["TEST_BUCKET_NAME"]

    def setUp(self):
        # Set up paths to the test data directory and to the app templates directory
        root_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_path = os.path.join(root_dir, "data")
        self.templates_path = os.path.join(os.path.dirname(root_dir), "octue", "templates")
        super().setUp()

    def callCli(self, args):
        """Utility to call the octue CLI (eg for a templated example) in a separate subprocess
        Enables testing that multiple processes aren't using the same memory space, or for running multiple apps in
        parallel to ensure they don't conflict
        """
        call_id = str(uuid.uuid4())
        tmp_dir_name = os.path.join(gettempdir(), "octue-sdk-python", f"test-{call_id}")

        with TemporaryDirectory(dir=tmp_dir_name):
            subprocess.call(args, cwd=tmp_dir_name)

    def create_valid_dataset(self):
        """ Create a valid dataset with two valid datafiles (they're the same file in this case). """
        path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        path = os.path.join("path-within-dataset", "a_test_file.csv")

        files = [
            Datafile(path_from=path_from, base_from=path_from, path=path, skip_checks=False),
            Datafile(path_from=path_from, base_from=path_from, path=path, skip_checks=False),
        ]

        return Dataset(files=files)

    def create_valid_manifest(self):
        """ Create a valid manifest with two valid datasets (they're the same dataset in this case). """
        datasets = [self.create_valid_dataset(), self.create_valid_dataset()]
        manifest = Manifest(datasets=datasets, keys={"my_dataset": 0, "another_dataset": 1})
        return manifest

    def make_new_server(self, backend, run_function_returnee, id=None):
        """ Make and return a new service ready to serve analyses from its run function. """
        run_function = lambda input_values, input_manifest: run_function_returnee  # noqa
        return Service(backend=backend, id=id or str(uuid.uuid4()), run_function=run_function)

    @staticmethod
    def create_google_cloud_test_bucket():
        """Create a Google Cloud bucket for testing."""
        storage.Client(
            project=os.environ["TEST_PROJECT_NAME"], credentials=GCPCredentialsManager().get_credentials()
        ).create_bucket(bucket_or_name=os.environ["TEST_BUCKET_NAME"])
