import copy
import json
import os
import tempfile
import uuid
from unittest.mock import patch

from octue import exceptions
from octue.cloud.storage import GoogleCloudStorageClient
from octue.mixins import MixinBase, Pathable
from octue.resources import Datafile
from octue.resources.tag import TagSet
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
        with self.assertRaises(TypeError) as error:
            Datafile(timestamp=None)

        self.assertIn("__init__() missing 1 required positional argument: 'path'", error.exception.args[0])

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
        """Ensure datafiles can be serialised to JSON."""
        serialised_datafile = self.create_valid_datafile().serialise()

        expected_fields = {
            "cluster",
            "id",
            "name",
            "path",
            "timestamp",
            "sequence",
            "tags",
            "hash_value",
            "_cloud_metadata",
        }

        self.assertEqual(serialised_datafile.keys(), expected_fields)

    def test_hash_value(self):
        """Test hashing a datafile gives a hash of length 8."""
        hash_ = self.create_valid_datafile().hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 8)

    def test_hashes_for_the_same_datafile_are_the_same(self):
        """Ensure the hashes for two datafiles that are exactly the same are the same."""
        first_file = self.create_valid_datafile()
        second_file = copy.deepcopy(first_file)
        self.assertEqual(first_file.hash_value, second_file.hash_value)

    def test_is_in_cloud(self):
        """Test whether a file is in the cloud or not can be determined."""
        self.assertFalse(self.create_valid_datafile().is_in_cloud)
        self.assertTrue(Datafile(timestamp=None, path="gs://hello/file.txt").is_in_cloud)

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

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        datafile = Datafile(
            timestamp=None, path=temporary_file.name, cluster=0, sequence=1, tags={"blah:shah:nah", "blib", "glib"}
        )
        datafile.to_cloud(project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket)

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

    def test_from_cloud_with_overwrite(self):
        """Test that a datafile can be instantiated from the cloud and have its attributes overwritten if new values
        are given in kwargs.
        """
        path_in_bucket = "file_to_upload.txt"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket
        )

        new_id = str(uuid.uuid4())

        new_datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            datafile_path=path_in_bucket,
            allow_overwrite=True,
            id=new_id,
        )

        self.assertEqual(new_datafile.id, new_id)

    def test_from_cloud_with_overwrite_when_disallowed_results_in_error(self):
        """Test that attempting to overwrite the attributes of a datafile instantiated from the cloud when not allowed
        results in an error.
        """
        path_in_bucket = "my-file.txt"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket
        )

        with self.assertRaises(exceptions.AttributeConflict):
            Datafile.from_cloud(
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
                datafile_path=path_in_bucket,
                allow_overwrite=False,
                id=str(uuid.uuid4()),
            )

    def test_to_cloud_updates_cloud_files(self):
        """Test that calling Datafile.to_cloud on a datafile that is already cloud-based updates it in the cloud."""
        path_in_bucket = "file_to_upload.txt"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        datafile = Datafile(timestamp=None, path=temporary_file.name, cluster=0)

        datafile.to_cloud(project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket)

        datafile.cluster = 3

        datafile.to_cloud(project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=path_in_bucket)

        self.assertEqual(
            Datafile.from_cloud(
                project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path=path_in_bucket
            ).cluster,
            3,
        )

    def test_get_local_path(self):
        """Test that a file in the cloud can be temporarily downloaded and its local path returned."""
        file_contents = "[1, 2, 3]"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(file_contents)

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket="nope.txt"
        )

        datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path="nope.txt"
        )

        with open(datafile.get_local_path()) as f:
            self.assertEqual(f.read(), file_contents)

    def test_get_local_path_with_cached_file_avoids_downloading_again(self):
        """Test that attempting to download a cached file avoids downloading it again."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket="nope.txt"
        )

        datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path="nope.txt"
        )

        # Download for first time.
        datafile.get_local_path()

        # Check that a new file isn't downloaded the second time.
        with patch("tempfile.NamedTemporaryFile") as temporary_file_mock:
            datafile.get_local_path()
            self.assertFalse(temporary_file_mock.called)

    def test_open_with_reading_local_file(self):
        """Test that a local datafile can be opened."""
        file_contents = "[1, 2, 3]"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(file_contents)

        datafile = Datafile(timestamp=None, path=temporary_file.name)

        with datafile.open() as f:
            self.assertEqual(f.read(), file_contents)

    def test_open_with_writing_local_file(self):
        """Test that a local datafile can be written to."""
        file_contents = "[1, 2, 3]"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(file_contents)

        datafile = Datafile(timestamp=None, path=temporary_file.name)

        with datafile.open("w") as f:
            f.write("hello")

        with datafile.open() as f:
            self.assertEqual(f.read(), "hello")

    def test_open_with_reading_cloud_file(self):
        """Test that a cloud datafile can be opened for reading."""
        file_contents = "[1, 2, 3]"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(file_contents)

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket="nope.txt"
        )

        datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path="nope.txt"
        )

        with datafile.open() as f:
            self.assertEqual(f.read(), file_contents)

    def test_open_with_writing_to_cloud_file(self):
        """Test that a cloud datafile can be opened for writing and that both the remote and local copies are updated."""
        original_file_contents = "[1, 2, 3]"
        filename = "nope.txt"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(original_file_contents)

        Datafile(timestamp=None, path=temporary_file.name).to_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, path_in_bucket=filename
        )

        datafile = Datafile.from_cloud(
            project_name=TEST_PROJECT_NAME, bucket_name=TEST_BUCKET_NAME, datafile_path=filename
        )

        new_file_contents = "nanana"

        with datafile.open("w") as f:
            f.write(new_file_contents)

            # Check that the cloud file isn't updated until the context manager is closed.
            self.assertEqual(
                GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME).download_as_string(
                    bucket_name=TEST_BUCKET_NAME, path_in_bucket=filename
                ),
                original_file_contents,
            )

        # Check that the cloud file has now been updated.
        self.assertEqual(
            GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME).download_as_string(
                bucket_name=TEST_BUCKET_NAME, path_in_bucket=filename
            ),
            new_file_contents,
        )

        # Check that the local copy has been updated.
        with datafile.open() as f:
            self.assertEqual(f.read(), new_file_contents)

    def test_deserialise(self):
        """Test that a Datafile can be deserialised faithfully."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("hello")

        datafile = Datafile(path=temporary_file.name, timestamp=None)
        serialised_datafile = datafile.serialise()
        deserialised_datafile = Datafile.deserialise(serialised_datafile)

        self.assertEqual(datafile.id, deserialised_datafile.id)
        self.assertEqual(datafile.name, deserialised_datafile.name)
        self.assertEqual(datafile.path, deserialised_datafile.path)
        self.assertEqual(datafile.absolute_path, deserialised_datafile.absolute_path)
        self.assertEqual(datafile.hash_value, deserialised_datafile.hash_value)
        self.assertEqual(datafile.size_bytes, deserialised_datafile.size_bytes)
        self.assertEqual(datafile.cluster, deserialised_datafile.cluster)
        self.assertEqual(datafile.sequence, deserialised_datafile.sequence)

    def test_deserialise_uses_path_from_if_path_is_relative(self):
        """Test that Datafile.deserialise uses the path_from parameter if the datafile's path is relative."""
        with tempfile.NamedTemporaryFile("w", dir=os.getcwd(), delete=False) as temporary_file:
            temporary_file.write("hello")

        filename = os.path.split(temporary_file.name)[-1]
        datafile = Datafile(path=filename, timestamp=None)
        serialised_datafile = datafile.serialise()

        pathable = Pathable(path=os.path.join(os.sep, "an", "absolute", "path"))
        deserialised_datafile = Datafile.deserialise(serialised_datafile, path_from=pathable)

        self.assertEqual(datafile.id, deserialised_datafile.id)
        self.assertEqual(deserialised_datafile.absolute_path, os.path.join(pathable.absolute_path, filename))

        os.remove(temporary_file.name)

    def test_deserialise_ignores_path_from_if_path_is_absolute(self):
        """Test that Datafile.deserialise ignores the path_from parameter if the datafile's path is absolute."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("hello")

        datafile = Datafile(path=temporary_file.name, timestamp=None)
        serialised_datafile = datafile.serialise()

        pathable = Pathable(path=os.path.join(os.sep, "an", "absolute", "path"))
        deserialised_datafile = Datafile.deserialise(serialised_datafile, path_from=pathable)

        self.assertEqual(datafile.id, deserialised_datafile.id)
        self.assertFalse(pathable.path in deserialised_datafile.path)
        self.assertEqual(deserialised_datafile.path, temporary_file.name)
