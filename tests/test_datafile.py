import os
import uuid

from octue import exceptions
from octue.resources import Datafile
from .base import BaseTestCase


class DatafileTestCase(BaseTestCase):

    LOCAL_PATH_PREFIX = os.path.join("basic_files", "configuration", "test-dataset")
    PATH = os.path.join("path-within-dataset", "a_test_file.csv")

    def create_valid_datafile(self):
        local_path_prefix = os.path.join(self.data_path, self.LOCAL_PATH_PREFIX)
        return Datafile(local_path_prefix=local_path_prefix, path=self.PATH, skip_checks=False)

    def test_instantiates(self):
        """ Ensures a Datafile instantiates using only a path and generates a uuid ID
        """
        df = Datafile(path="a_path")
        self.assertTrue(isinstance(df.id, str))
        self.assertEqual(type(uuid.UUID(df.id)), uuid.UUID)
        self.assertIsNone(df.sequence)
        self.assertEqual(0, df.cluster)

    def test_path_argument_required(self):
        """ Ensures instantiation without a path will fail
        """
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Datafile()

        self.assertIn("You must supply a valid 'path' argument", error.exception.args[0])

    def test_default_local_path_prefix(self):
        """ Ensures the local path, by default, is set to the current working directory
        """
        df = Datafile(path=os.path.join("/", self.PATH), skip_checks=True)
        self.assertEqual(self.PATH, df.path)
        self.assertEqual(str(os.getcwd()), df.local_path_prefix.rstrip("\\/"))

    def test_local_path_prefix_with_checks(self):
        """ Ensures the local path can be specified and that checks pass on instantiation
        """
        local_path = os.path.join(self.data_path, self.LOCAL_PATH_PREFIX)
        df = Datafile(path=self.PATH, local_path_prefix=local_path, skip_checks=False)
        self.assertEqual("csv", df.extension)

    def test_leading_slashes_on_path_are_stripped(self):
        """ Ensures the path is always relative
        """
        df = Datafile(path=os.path.join("/", self.PATH), skip_checks=True)
        self.assertEqual(self.PATH, df.path)

        df = Datafile(path=self.PATH, skip_checks=True)
        self.assertEqual(self.PATH, df.path)

    def test_checks_pass_when_file_exists(self):
        df = self.create_valid_datafile()
        self.assertEqual(self.PATH, df.path)
        self.assertEqual(os.path.join(self.data_path, self.LOCAL_PATH_PREFIX, self.PATH), df.full_path)
        self.assertEqual("csv", df.extension)

    def test_checks_fail_when_file_doesnt_exist(self):
        path = "not_a_real_file.csv"
        with self.assertRaises(exceptions.FileNotFoundException) as error:
            Datafile(path=path, skip_checks=False)
        self.assertIn("No file found at", error.exception.args[0])

    def test_conflicting_extension_fails_check(self):
        local_path_prefix = os.path.join(self.data_path, self.LOCAL_PATH_PREFIX)
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Datafile(local_path_prefix=local_path_prefix, path=self.PATH, skip_checks=False, extension="notcsv")

        self.assertIn("Extension provided (notcsv) does not match file extension", error.exception.args[0])

    def test_file_attributes_accessible(self):
        """ Ensures that its possible to set the sequence, cluster and timestamp
        """
        df = self.create_valid_datafile()
        self.assertIsInstance(df.size_bytes, int)
        self.assertGreaterEqual(df.last_modified, 1598200190.5771205)
        self.assertEqual("a_test_file.csv", df.name)

        df.sequence = 2
        df.cluster = 0
        df.posix_timestamp = 0

    def test_cannot_set_calculated_file_attributes(self):
        """ Ensures that calculated attributes cannot be set
        """
        df = self.create_valid_datafile()

        with self.assertRaises(AttributeError):
            df.size_bytes = 1

        with self.assertRaises(AttributeError):
            df.last_modified = 1000000000.5771205

    def test_serialisable(self):
        """ Ensures a datafile can serialise to json format
        """
        df = self.create_valid_datafile()
        df_dict = df.serialise()

        for k in df_dict.keys():
            self.assertFalse(k.startswith("_"))

        for k in (
            "cluster",
            "extension",
            "id",
            "last_modified",
            "name",
            "path",
            "posix_timestamp",
            "sequence",
            "size_bytes",
            "tags",
            "sha_256",
        ):
            self.assertIn(k, df_dict.keys())
