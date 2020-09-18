import os
import shutil
import uuid

from octue import Runner, exceptions
from octue.runner import FOLDERS
from .base import BaseTestCase


class RunnerTestCase(BaseTestCase):
    """ Tests the Runner class
    """

    def test_instantiate_runner_with_string_paths(self):
        """ Ensures that error is raised if instantiated with a string path that's not present
        """
        runner = Runner(twine="{}", paths=os.path.join(self.data_path, "basic_empty"))
        self.assertEqual(runner.__class__.__name__, "Runner")

    def test_instantiate_runner_with_invalid_paths(self):
        """ Ensures that error is raised if instantiated with a string path that's not present
        """
        with self.assertRaises(exceptions.FolderNotFoundException):
            Runner(paths="floopitygibbet")

    def test_instantiate_runner_with_valid_dict_paths(self):
        """ Ensures that error is raised if instantiated with a string path that's not present
        """
        paths = dict([(k, os.path.join(self.data_path, "basic_empty", k)) for k in FOLDERS])
        Runner(twine="{}", paths=paths)

    def test_instantiate_runner_with_incomplete_dict_paths(self):
        """ Ensures that error is raised if instantiated with a string path that's not present
        """
        with self.assertRaises(exceptions.InvalidInputException):
            Runner(paths={"wrong", "a path"})

    def test_runner_instantiaties_if_path_subdirectories_if_not_present(self):
        """ Ensures that subdirectories are created in the data dir
        """
        # Make a data directory (uuid so this test doesn't conflict
        new_path = os.path.join(self.data_path, str(uuid.uuid4()))
        os.mkdir(new_path)

        try:
            Runner(twine="{}", paths=new_path)
            for folder in FOLDERS:
                self.assertTrue(os.path.isdir(os.path.join(new_path, folder)))
        finally:
            shutil.rmtree(new_path)

    def test_runner_instantiates_if_path_subdirectories_present(self):
        """ Ensures that if subdirectories are already present in the data directory there is no failure
        """
        # Make a data directory (uuid so this test doesn't conflict)
        new_path = os.path.join(self.data_path, str(uuid.uuid4()))
        os.mkdir(new_path)
        for folder in ["configuration", "tmp", "input", "output", "log"]:
            os.mkdir(os.path.join(new_path, folder))

        try:
            runner = Runner(twine="{}", paths=new_path)
            self.assertEqual(runner.__class__.__name__, "Runner")
        finally:
            shutil.rmtree(new_path)
