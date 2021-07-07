import copy
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

from octue import exceptions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.mixins import MixinBase, Pathable
from octue.resources.datafile import TEMPORARY_LOCAL_FILE_CACHE, Datafile
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from ..base import BaseTestCase


class MyPathable(Pathable, MixinBase):
    pass


class DatafileTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        self.path = os.path.join("path-within-dataset", "a_test_file.csv")

    def tearDown(self):
        TEMPORARY_LOCAL_FILE_CACHE.clear()

    def create_valid_datafile(self):
        return Datafile(path_from=self.path_from, path=self.path, skip_checks=False)

    def create_datafile_in_cloud(
        self,
        project_name=TEST_PROJECT_NAME,
        bucket_name=TEST_BUCKET_NAME,
        path_in_bucket="cloud_file.txt",
        contents="some text",
        **kwargs,
    ):
        """Create a datafile in the cloud. Any metadata attributes can be set via kwargs.

        :param str project_name:
        :param str bucket_name:
        :param str path_in_bucket:
        :param str contents:
        :return (str, str, str, str):
        """
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(contents)

        datafile = Datafile(path=temporary_file.name, **kwargs)
        datafile.to_cloud(project_name=project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket)
        return datafile, project_name, bucket_name, path_in_bucket, contents

    def test_instantiates(self):
        """Ensures a Datafile instantiates using only a path and generates a uuid ID"""
        df = Datafile(path="a_path")
        self.assertTrue(isinstance(df.id, str))
        self.assertEqual(type(uuid.UUID(df.id)), uuid.UUID)

    def test_path_argument_required(self):
        """Ensures instantiation without a path will fail"""
        with self.assertRaises(TypeError) as error:
            Datafile(timestamp=None)

        self.assertIn("__init__() missing 1 required positional argument: 'path'", error.exception.args[0])

    def test_setting_timestamp(self):
        """Test that both datetime and posix timestamps can be used for a Datafile, that the timestamp attribute is
        always converted to a datetime instance, and that invalid timestamps raise an error.
        """
        self.assertTrue(isinstance(Datafile(timestamp=datetime.now(), path="a_path").timestamp, datetime))
        self.assertTrue(isinstance(Datafile(timestamp=50, path="a_path").timestamp, datetime))

        with self.assertRaises(TypeError):
            Datafile(timestamp="50", path="a_path")

    def test_gt(self):
        """Test that datafiles can be ordered using the greater-than operator."""
        a = Datafile(path="a_path")
        b = Datafile(path="b_path")
        self.assertTrue(a < b)

    def test_gt_with_wrong_type(self):
        """Test that datafiles cannot be ordered compared to other types."""
        with self.assertRaises(TypeError):
            Datafile(path="a_path") < "hello"

    def test_lt(self):
        """Test that datafiles can be ordered using the less-than operator."""
        a = Datafile(path="a_path")
        b = Datafile(path="b_path")
        self.assertTrue(b > a)

    def test_lt_with_wrong_type(self):
        """Test that datafiles cannot be ordered compared to other types."""
        with self.assertRaises(TypeError):
            Datafile(path="a_path") > "hello"

    def test_checks_fail_when_file_doesnt_exist(self):
        path = "not_a_real_file.csv"
        with self.assertRaises(exceptions.FileNotFoundException) as error:
            Datafile(path=path, skip_checks=False)
        self.assertIn("No file found at", error.exception.args[0])

    def test_conflicting_extension_fails_check(self):
        with self.assertRaises(exceptions.InvalidInputException) as error:
            Datafile(path_from=self.path_from, path=self.path, skip_checks=False, extension="notcsv")

        self.assertIn("Extension provided (notcsv) does not match file extension", error.exception.args[0])

    def test_file_attributes_accessible(self):
        """Ensures that its possible to set the timestamp"""
        df = self.create_valid_datafile()
        self.assertIsInstance(df.size_bytes, int)
        self.assertGreaterEqual(df._last_modified, 1598200190.5771205)
        self.assertEqual("a_test_file.csv", df.name)

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
            "id",
            "name",
            "path",
            "timestamp",
            "tags",
            "labels",
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
        self.assertTrue(Datafile(path="gs://hello/file.txt", hypothetical=True).is_in_cloud)

    def test_from_cloud_with_bare_file(self):
        """Test that a Datafile can be constructed from a file on Google Cloud storage with no custom metadata."""
        path_in_bucket = "file_to_upload.txt"

        GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME).upload_from_string(
            string=json.dumps({"height": 32}),
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=path_in_bucket,
        )

        datafile = Datafile(
            path=storage.path.generate_gs_path(TEST_BUCKET_NAME, path_in_bucket), project_name=TEST_PROJECT_NAME
        )

        self.assertEqual(datafile.path, f"gs://{TEST_BUCKET_NAME}/{path_in_bucket}")
        self.assertEqual(datafile.tags, TagDict())
        self.assertEqual(datafile.labels, LabelSet())
        self.assertTrue(isinstance(datafile.size_bytes, int))
        self.assertTrue(isinstance(datafile._last_modified, float))
        self.assertTrue(isinstance(datafile.hash_value, str))

    def test_from_cloud_with_datafile(self):
        """Test that a Datafile can be constructed from a file on Google Cloud storage with custom metadata."""
        datafile, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud(
            timestamp=datetime.now(tz=timezone.utc),
            labels={"blah-shah-nah", "blib", "glib"},
            tags={"good": True, "how_good": "very"},
        )

        gs_path = f"gs://{TEST_BUCKET_NAME}/{path_in_bucket}"

        downloaded_datafile = Datafile(path=gs_path, project_name=project_name)
        self.assertEqual(downloaded_datafile.path, gs_path)
        self.assertEqual(downloaded_datafile.id, datafile.id)
        self.assertEqual(downloaded_datafile.timestamp, datafile.timestamp)
        self.assertEqual(downloaded_datafile.hash_value, datafile.hash_value)
        self.assertEqual(downloaded_datafile.tags, datafile.tags)
        self.assertEqual(downloaded_datafile.labels, datafile.labels)
        self.assertEqual(downloaded_datafile.size_bytes, datafile.size_bytes)
        self.assertTrue(isinstance(downloaded_datafile._last_modified, float))

    def test_to_cloud_updates_cloud_metadata(self):
        """Test that calling Datafile.to_cloud on a datafile that is already cloud-based updates its metadata in the
        cloud.
        """
        datafile, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud(labels={"start"})

        datafile.labels = {"finish"}
        datafile.to_cloud(project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket)

        self.assertEqual(
            Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name).labels,
            {"finish"},
        )

    def test_to_cloud_does_not_update_cloud_metadata_if_update_cloud_metadata_is_false(self):
        """Test that calling Datafile.to_cloud with `update_cloud_metadata=False` doesn't update the cloud metadata."""
        datafile, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud(labels={"start"})
        datafile.labels = {"finish"}

        with patch("octue.resources.datafile.Datafile.update_cloud_metadata") as mock:
            datafile.to_cloud(
                project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket, update_cloud_metadata=False
            )
            self.assertFalse(mock.called)

        self.assertEqual(
            Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name).labels,
            {"start"},
        )

    def test_to_cloud_does_not_update_metadata_if_no_metadata_change_has_been_made(self):
        """Test that Datafile.to_cloud does not try to update cloud metadata if no metadata change has been made."""
        _, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud(labels={"start"})

        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)

        with patch("octue.resources.datafile.Datafile.update_cloud_metadata") as mock:
            datafile.to_cloud()
            self.assertFalse(mock.called)

    def test_to_cloud_raises_error_if_no_cloud_location_provided_and_datafile_not_from_cloud(self):
        """Test that trying to send a datafile to the cloud with no cloud location provided when the datafile was not
        constructed from a cloud file results in cloud location error.
        """
        datafile = Datafile(path="hello.txt", timestamp=None)

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            datafile.to_cloud()

    def test_to_cloud_works_with_implicit_cloud_location_if_cloud_location_previously_provided(self):
        """Test datafile.to_cloud works with an implicit cloud location if the cloud location has previously been
        provided.
        """
        _, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud()
        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)
        datafile.to_cloud()

    def test_to_cloud_does_not_try_to_update_file_if_no_change_has_been_made_locally(self):
        """Test that Datafile.to_cloud does not try to update cloud file if no change has been made locally."""
        datafile, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud(labels={"start"})

        with patch("octue.cloud.storage.client.GoogleCloudStorageClient.upload_file") as mock:
            datafile.to_cloud()
            self.assertFalse(mock.called)

    def test_update_cloud_metadata(self):
        """Test that a cloud datafile's metadata can be updated."""
        _, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud()

        new_datafile = Datafile(path="glib.txt", labels={"new"})
        new_datafile.update_cloud_metadata(project_name, bucket_name=bucket_name, path_in_bucket=path_in_bucket)

        self.assertEqual(
            Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name).labels,
            {"new"},
        )

    def test_update_cloud_metadata_works_with_implicit_cloud_location_if_cloud_location_previously_provided(self):
        """Test that datafile.update_metadata works with an implicit cloud location if the cloud location has been
        previously provided.
        """
        _, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud()

        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)
        datafile.labels = {"new"}
        datafile.update_cloud_metadata()

        self.assertEqual(
            Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name).labels,
            {"new"},
        )

    def test_update_cloud_metadata_raises_error_if_no_cloud_location_provided_and_datafile_not_from_cloud(self):
        """Test that trying to update a cloud datafile's metadata with no cloud location provided when the datafile was
        not constructed from a cloud file results in cloud location error.
        """
        datafile = Datafile(path="hello.txt", timestamp=None)

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            datafile.update_cloud_metadata()

    def test_get_local_path(self):
        """Test that a file in the cloud can be temporarily downloaded and its local path returned."""
        _, project_name, bucket_name, path_in_bucket, contents = self.create_datafile_in_cloud()
        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)

        with open(datafile.get_local_path()) as f:
            self.assertEqual(f.read(), contents)

    def test_get_local_path_with_cached_file_avoids_downloading_again(self):
        """Test that attempting to download a cached file avoids downloading it again."""
        _, project_name, bucket_name, path_in_bucket, _ = self.create_datafile_in_cloud()
        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)

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

        datafile = Datafile(path=temporary_file.name)

        with datafile.open() as f:
            self.assertEqual(f.read(), file_contents)

    def test_open_with_writing_local_file(self):
        """Test that a local datafile can be written to."""
        file_contents = "[1, 2, 3]"

        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(file_contents)

        datafile = Datafile(path=temporary_file.name)

        with datafile.open("w") as f:
            f.write("hello")

        with datafile.open() as f:
            self.assertEqual(f.read(), "hello")

    def test_open_with_reading_cloud_file(self):
        """Test that a cloud datafile can be opened for reading."""
        _, project_name, bucket_name, path_in_bucket, contents = self.create_datafile_in_cloud()
        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)

        with datafile.open() as f:
            self.assertEqual(f.read(), contents)

    def test_open_with_writing_to_cloud_file(self):
        """Test that a cloud datafile can be opened for writing and that both the remote and local copies are updated."""
        _, project_name, bucket_name, path_in_bucket, original_contents = self.create_datafile_in_cloud()
        datafile = Datafile(storage.path.generate_gs_path(bucket_name, path_in_bucket), project_name=project_name)

        new_file_contents = "nanana"

        with datafile.open("w") as f:
            f.write(new_file_contents)

            # Check that the cloud file isn't updated until the context manager is closed.
            self.assertEqual(
                GoogleCloudStorageClient(project_name=project_name).download_as_string(
                    bucket_name=bucket_name, path_in_bucket=path_in_bucket
                ),
                original_contents,
            )

        # Check that the cloud file has now been updated.
        self.assertEqual(
            GoogleCloudStorageClient(project_name=project_name).download_as_string(
                bucket_name=bucket_name, path_in_bucket=path_in_bucket
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

    def test_posix_timestamp(self):
        """Test that the posix timestamp property works properly."""
        datafile = Datafile(path="hello.txt", timestamp=None)
        self.assertIsNone(datafile.posix_timestamp)

        datafile.timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(datafile.posix_timestamp, 0)

    def test_datafile_as_context_manager(self):
        """Test that Datafile can be used as a context manager to manage local changes."""
        temporary_file = tempfile.NamedTemporaryFile("w", delete=False)
        contents = "Here is the content."

        with Datafile(path=temporary_file.name, mode="w") as (datafile, f):
            f.write(contents)

        # Check that the cloud file has been updated.
        with datafile.open() as f:
            self.assertEqual(f.read(), contents)

    def test_from_datafile_as_context_manager(self):
        """Test that Datafile can be used as a context manager to manage cloud changes."""
        _, project_name, bucket_name, path_in_bucket, original_content = self.create_datafile_in_cloud()
        new_contents = "Here is the new content."
        self.assertNotEqual(original_content, new_contents)

        path = storage.path.generate_gs_path(bucket_name, path_in_bucket)

        with Datafile(path, project_name=project_name, mode="w") as (datafile, f):
            datafile.add_labels("blue")
            f.write(new_contents)

        # Check that the cloud metadata has been updated.
        re_downloaded_datafile = Datafile(path, project_name=project_name)
        self.assertTrue("blue" in re_downloaded_datafile.labels)

        # The file cache must be cleared so the modified cloud file is downloaded.
        re_downloaded_datafile.clear_from_file_cache()

        # Check that the cloud file has been updated.
        with re_downloaded_datafile.open() as f:
            self.assertEqual(f.read(), new_contents)

    def test_metadata(self):
        """Test that the metadata method namespaces the metadata names when required."""
        datafile = self.create_valid_datafile()

        self.assertEqual(
            datafile.metadata().keys(),
            {
                "octue__id",
                "octue__timestamp",
                "octue__tags",
                "octue__labels",
                "octue__sdk_version",
            },
        )

        self.assertEqual(
            datafile.metadata(use_octue_namespace=False).keys(),
            {"id", "timestamp", "tags", "labels", "sdk_version"},
        )

    def test_creating_new_cloud_datafile_without_local_file(self):
        """Test that a new datafile can be created in the cloud without making a local file first."""
        path = f"gs://{TEST_BUCKET_NAME}/new_cloud_file.txt"

        with Datafile(path=path, project_name=TEST_PROJECT_NAME, mode="w") as (datafile, f):
            f.write('{"my": "data"}')

        data = GoogleCloudStorageClient(TEST_PROJECT_NAME).download_as_string(path)
        self.assertEqual(data, '{"my": "data"}')
