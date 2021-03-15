import copy
import json
import os
import tempfile
import uuid

from octue import exceptions
from octue.mixins import MixinBase, Pathable
from octue.resources import Datafile
from octue.resources.tag import TagSet
from octue.utils.cloud.storage.client import GoogleCloudStorageClient
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from ..base import BaseTestCase


class MyPathable(Pathable, MixinBase):
    pass


class DatafileTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        self.path = os.path.join("path-within-dataset", "a_test_file.csv")

    def create_valid_datafile(self):
        return Datafile(timestamp=None, path_from=self.path_from, path=self.path, skip_checks=False)

    def test_instantiates(self):
        """Ensures a Datafile instantiates using only a path and generates a uuid ID"""
        df = Datafile(timestamp=None, path="a_path")
        self.assertTrue(isinstance(df.id, str))
        self.assertEqual(type(uuid.UUID(df.id)), uuid.UUID)
        self.assertIsNone(df.sequence)
        self.assertEqual(0, df.cluster)

    def test_path_argument_required(self):
        """Ensures instantiation without a path will fail"""
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Datafile(timestamp=None)

        self.assertIn("You must supply a valid 'path' for a Datafile", error.exception.args[0])

    def test_gt(self):
        """Test that datafiles can be ordered using the greater-than operator."""
        a = Datafile(timestamp=None, path="a_path")
        b = Datafile(timestamp=None, path="b_path")
        self.assertTrue(a < b)

    def test_gt_with_wrong_type(self):
        """Test that datafiles cannot be ordered compared to other types."""
        with self.assertRaises(TypeError):
            Datafile(timestamp=None, path="a_path") < "hello"

    def test_lt(self):
        """Test that datafiles can be ordered using the less-than operator."""
        a = Datafile(timestamp=None, path="a_path")
        b = Datafile(timestamp=None, path="b_path")
        self.assertTrue(b > a)

    def test_lt_with_wrong_type(self):
        """Test that datafiles cannot be ordered compared to other types."""
        with self.assertRaises(TypeError):
            Datafile(timestamp=None, path="a_path") > "hello"

    def test_checks_fail_when_file_doesnt_exist(self):
        path = "not_a_real_file.csv"
        with self.assertRaises(exceptions.FileNotFoundException) as error:
            Datafile(timestamp=None, path=path, skip_checks=False)
        self.assertIn("No file found at", error.exception.args[0])

    def test_conflicting_extension_fails_check(self):
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Datafile(timestamp=None, path_from=self.path_from, path=self.path, skip_checks=False, extension="notcsv")

        self.assertIn("Extension provided (notcsv) does not match file extension", error.exception.args[0])

    def test_file_attributes_accessible(self):
        """Ensures that its possible to set the sequence, cluster and timestamp"""
        df = self.create_valid_datafile()
        self.assertIsInstance(df.size_bytes, int)
        self.assertGreaterEqual(df._last_modified, 1598200190.5771205)
        self.assertEqual("a_test_file.csv", df.name)

        df.sequence = 2
        df.cluster = 0
        df.timestamp = 0

    def test_cannot_set_calculated_file_attributes(self):
        """Ensures that calculated attributes cannot be set"""
        df = self.create_valid_datafile()

        with self.assertRaises(AttributeError):
            df.size_bytes = 1

        with self.assertRaises(AttributeError):
            df._last_modified = 1000000000.5771205

    def test_repr(self):
        """ Test that Datafiles are represented as expected. """
        self.assertEqual(repr(self.create_valid_datafile()), "<Datafile('a_test_file.csv')>")

    def test_serialisable(self):
        """Ensures a datafile can serialise to json format"""
        df = self.create_valid_datafile()
        df_dict = df.serialise()

        for k in df_dict.keys():
            self.assertFalse(k.startswith("_"))

        for k in (
            "cluster",
            "extension",
            "id",
            "name",
            "path",
            "timestamp",
            "sequence",
            "size_bytes",
            "tags",
            "hash_value",
        ):
            self.assertIn(k, df_dict.keys())

    def test_hash_value(self):
        """ Test hashing a datafile gives a hash of length 128. """
        hash_ = self.create_valid_datafile().hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 64)

    def test_hashes_for_the_same_datafile_are_the_same(self):
        """ Ensure the hashes for two datafiles that are exactly the same are the same."""
        first_file = self.create_valid_datafile()
        second_file = copy.deepcopy(first_file)
        self.assertEqual(first_file.hash_value, second_file.hash_value)

    def test_from_cloud_with_bare_file(self):
        """Test that a Datafile can be constructed from a file on Google Cloud storage with no custom metadata."""
        path_in_bucket = "file_to_upload.txt"

        GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME).upload_from_string(
            string=json.dumps({"height": 32}),
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=path_in_bucket,
        )

        datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path=path_in_bucket, timestamp=None
        )

        self.assertEqual(datafile.path, f"gs://{TEST_BUCKET_NAME}/{path_in_bucket}")
        self.assertEqual(datafile.cluster, 0)
        self.assertEqual(datafile.sequence, None)
        self.assertEqual(datafile.tags, TagSet())
        self.assertTrue(isinstance(datafile.size_bytes, int))
        self.assertTrue(isinstance(datafile._last_modified, float))
        self.assertTrue(isinstance(datafile.hash_value, str))

    def test_from_cloud_with_datafile(self):
        """Test that a Datafile can be constructed from a file on Google Cloud storage with custom metadata."""
        path_in_bucket = "file_to_upload.txt"

        with tempfile.TemporaryDirectory() as temporary_directory:
            file_0_path = os.path.join(temporary_directory, "file_0.txt")

            with open(file_0_path, "w") as f:
                f.write("[1, 2, 3]")

            datafile = Datafile(
                timestamp=None, path=file_0_path, cluster=0, sequence=1, tags={"blah:shah:nah", "blib", "glib"}
            )
            datafile.to_cloud(
                project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket
            )

            persisted_datafile = Datafile.from_cloud(
                project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path=path_in_bucket
            )

            self.assertEqual(persisted_datafile.path, f"gs://{TEST_BUCKET_NAME}/{path_in_bucket}")
            self.assertEqual(persisted_datafile.id, datafile.id)
            self.assertEqual(persisted_datafile.hash_value, datafile.hash_value)
            self.assertEqual(persisted_datafile.cluster, datafile.cluster)
            self.assertEqual(persisted_datafile.sequence, datafile.sequence)
            self.assertEqual(persisted_datafile.tags, datafile.tags)
            self.assertEqual(persisted_datafile.size_bytes, datafile.size_bytes)
            self.assertTrue(isinstance(persisted_datafile._last_modified, float))
