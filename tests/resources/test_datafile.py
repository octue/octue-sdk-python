import copy
import json
import os
import pickle
import tempfile
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import h5py
import pkg_resources

from octue import exceptions
from octue.cloud import storage
from octue.cloud.storage import GoogleCloudStorageClient
from octue.mixins import MixinBase, Pathable
from octue.resources.datafile import Datafile
from octue.resources.label import LabelSet
from octue.resources.tag import TagDict
from tests import TEST_BUCKET_NAME

from ..base import BaseTestCase


class MyPathable(Pathable, MixinBase):
    pass


class TestDatafile(BaseTestCase):
    def setUp(self):
        """Set up the test class by adding an example `path_from` and `path` to it.

        :return None:
        """
        super().setUp()
        self.path_from = MyPathable(path=os.path.join(self.data_path, "basic_files", "configuration", "test-dataset"))
        self.path = os.path.join("path-within-dataset", "a_test_file.csv")

    def create_valid_datafile(self):
        """Create a datafile with its `path_from` and `path` attributes set to valid values.

        :return octue.resources.datafile.Datafile:
        """
        return Datafile(path_from=self.path_from, path=self.path, skip_checks=False)

    def create_datafile_in_cloud(
        self,
        cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, "cloud_file.txt"),
        contents="some text",
        **kwargs,
    ):
        """Create a datafile in the cloud. Any metadata attributes can be set via kwargs.

        :param str cloud_path:
        :param str contents:
        :return (octue.resources.datafile.Datafile, str):
        """
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write(contents)

        datafile = Datafile(path=temporary_file.name, **kwargs)
        datafile.to_cloud(cloud_path=cloud_path)
        return datafile, contents

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
        """Test that the checks fail if the file used to instantiate the datafile doesn't exist."""
        path = "not_a_real_file.csv"
        with self.assertRaises(exceptions.FileNotFoundException) as error:
            Datafile(path=path, skip_checks=False)
        self.assertIn("No file found at", error.exception.args[0])

    def test_conflicting_extension_fails_check(self):
        """Test that a conflicting extension parameter and extension on the file causes checks to fail."""
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
        """Test that Datafiles are represented as expected."""
        self.assertEqual(repr(self.create_valid_datafile()), "<Datafile('a_test_file.csv')>")

    def test_serialisable(self):
        """Ensure datafiles can be serialised to JSON."""
        serialised_datafile = self.create_valid_datafile().to_primitive()

        expected_fields = {
            "id",
            "name",
            "path",
            "cloud_path",
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

    def test_hash_only_depends_on_file_contents(self):
        """Test that the hash of a datafile depends only on its file contents and not on e.g. its name or ID."""
        first_file = self.create_valid_datafile()
        second_file = copy.deepcopy(first_file)
        second_file._name = "different"
        second_file._id = "51f62f08-112c-44cb-9468-3e856d30a7ff"
        self.assertEqual(first_file.hash_value, second_file.hash_value)

    def test_hash_of_cloud_datafile_avoids_downloading_file(self):
        """Test that getting/calculating the hash of a cloud datafile avoids downloading its contents."""
        cloud_datafile, _ = self.create_datafile_in_cloud()
        datafile_reloaded_from_cloud = Datafile(path=cloud_datafile.cloud_path)

        # Calculate the hashes of the datafiles and check that they're equal.
        self.assertEqual(datafile_reloaded_from_cloud.hash_value, cloud_datafile.hash_value)

        # Check that the reloaded datafile hasn't been downloaded.
        self.assertIsNone(datafile_reloaded_from_cloud._local_path)

    def test_exists_in_cloud(self):
        """Test whether it can be determined that a datafile exists in the cloud or not."""
        self.assertFalse(self.create_valid_datafile().exists_in_cloud)
        self.assertTrue(Datafile(path="gs://hello/file.txt", hypothetical=True).exists_in_cloud)

    def test_exists_locally(self):
        """Test whether it can be determined that a datafile exists locally or not."""
        self.assertTrue(self.create_valid_datafile().exists_locally)
        self.assertFalse(Datafile(path="gs://hello/file.txt", hypothetical=True).exists_locally)

        datafile, _ = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.cloud_path)

        # Ensure the datafile exists locally as well as in the cloud.
        new_datafile.download()
        self.assertTrue(new_datafile.exists_locally)

    def test_from_cloud_with_bare_file(self):
        """Test that a Datafile can be constructed from a bare Google Cloud Storage object with no custom metadata."""
        path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "file_to_upload.txt")

        GoogleCloudStorageClient().upload_from_string(string=json.dumps({"height": 32}), cloud_path=path)

        datafile = Datafile(path=path)

        self.assertEqual(datafile.path, path)
        self.assertEqual(datafile.tags, TagDict())
        self.assertEqual(datafile.labels, LabelSet())
        self.assertTrue(isinstance(datafile.size_bytes, int))
        self.assertTrue(isinstance(datafile._last_modified, float))
        self.assertTrue(isinstance(datafile.hash_value, str))

    def test_from_cloud_with_bare_file_setting_metadata_at_instantiation(self):
        """Test that a datafile can be constructed from a bare Google Cloud Storage object and have its metadata set to
        custom values at instantiation.
        """
        path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "file_to_upload.txt")

        GoogleCloudStorageClient().upload_from_string(
            string=json.dumps({"height": 32}),
            cloud_path=path,
        )

        datafile_id = str(uuid.uuid4())
        timestamp = datetime.now()
        tags = {"a": 1}
        labels = {"pink"}

        datafile = Datafile(path=path, id=datafile_id, timestamp=timestamp, tags=tags, labels=labels)

        self.assertEqual(datafile.path, path)
        self.assertEqual(datafile.id, datafile_id)
        self.assertEqual(datafile.timestamp, timestamp)
        self.assertEqual(datafile.tags, tags)
        self.assertEqual(datafile.labels, labels)
        self.assertTrue(isinstance(datafile.size_bytes, int))
        self.assertTrue(isinstance(datafile._last_modified, float))
        self.assertTrue(isinstance(datafile.hash_value, str))

    def test_from_cloud_with_datafile(self):
        """Test that a Datafile can be constructed from a file on Google Cloud storage with custom metadata."""
        datafile, _ = self.create_datafile_in_cloud(
            timestamp=datetime.now(tz=timezone.utc),
            labels={"blah-shah-nah", "blib", "glib"},
            tags={"good": True, "how_good": "very"},
        )

        downloaded_datafile = Datafile(path=datafile.cloud_path)
        self.assertEqual(downloaded_datafile.path, datafile.cloud_path)
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
        datafile, _ = self.create_datafile_in_cloud(labels={"start"})
        datafile.labels = {"finish"}
        datafile.to_cloud(cloud_path=datafile.cloud_path)

        self.assertEqual(Datafile(datafile.cloud_path).labels, {"finish"})

    def test_to_cloud_does_not_update_cloud_metadata_if_update_cloud_metadata_is_false(self):
        """Test that calling Datafile.to_cloud with `update_cloud_metadata=False` doesn't update the cloud metadata."""
        datafile, _ = self.create_datafile_in_cloud(labels={"start"})
        datafile.labels = {"finish"}

        with patch("octue.resources.datafile.Datafile._update_cloud_metadata") as mock:
            datafile.to_cloud(datafile.cloud_path, update_cloud_metadata=False)
            self.assertFalse(mock.called)

        self.assertEqual(Datafile(datafile.cloud_path).labels, {"start"})

    def test_to_cloud_does_not_update_metadata_if_no_metadata_change_has_been_made(self):
        """Test that Datafile.to_cloud does not try to update cloud metadata if no metadata change has been made."""
        datafile, _ = self.create_datafile_in_cloud(labels={"start"})

        new_datafile = Datafile(datafile.cloud_path)

        with patch("octue.resources.datafile.Datafile._update_cloud_metadata") as mock:
            new_datafile.to_cloud()
            self.assertFalse(mock.called)

    def test_to_cloud_raises_error_if_no_cloud_location_provided_and_datafile_not_from_cloud(self):
        """Test that trying to send a datafile to the cloud with no cloud location provided when the datafile was not
        constructed from a cloud file results in cloud location error.
        """
        datafile = Datafile(path="hello.txt")

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            datafile.to_cloud()

    def test_to_cloud_works_with_implicit_cloud_location_if_cloud_location_previously_provided(self):
        """Test datafile.to_cloud works with an implicit cloud location if the cloud location has previously been
        provided.
        """
        datafile, _ = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.cloud_path)
        new_datafile.to_cloud()

    def test_to_cloud_does_not_try_to_update_file_if_no_change_has_been_made_locally(self):
        """Test that Datafile.to_cloud does not try to update cloud file if no change has been made locally."""
        datafile, _ = self.create_datafile_in_cloud(labels={"start"})

        with patch("octue.cloud.storage.client.GoogleCloudStorageClient.upload_file") as mock:
            datafile.to_cloud()
            self.assertFalse(mock.called)

    def test_update_cloud_metadata(self):
        """Test that a datafile's cloud metadata can be updated."""
        datafile, _ = self.create_datafile_in_cloud()

        new_datafile = Datafile(datafile.cloud_path)
        new_datafile.labels = {"new"}
        new_datafile._update_cloud_metadata()

        self.assertEqual(Datafile(datafile.cloud_path).labels, {"new"})

    def test_update_cloud_metadata_raises_error_if_datafile_does_not_exist_in_cloud(self):
        """Test that trying to update a datafile's metadata when it does not exist in the cloud results in a cloud
        location error.
        """
        datafile = Datafile(path="hello.txt")

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            datafile._update_cloud_metadata()

    def test_cloud_path(self):
        """Test that the cloud path property gives the right path."""
        datafile, _ = self.create_datafile_in_cloud()
        new_datafile = Datafile(path=datafile.cloud_path)
        self.assertEqual(new_datafile.cloud_path, datafile.cloud_path)

    def test_cloud_path_is_none_for_local_files(self):
        """Test that the cloud path property is `None` for local-only datafiles."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("[1, 2, 3]")

        datafile = Datafile(path=temporary_file.name)
        self.assertIsNone(datafile.cloud_path)

    def test_local_path(self):
        """Test that a file in the cloud can be temporarily downloaded and its local path returned."""
        datafile, contents = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.path)

        with open(new_datafile.local_path) as f:
            self.assertEqual(f.read(), contents)

    def test_local_path_with_cached_file_avoids_downloading_again(self):
        """Test that attempting to download a cached file doesn't result in a new download."""
        datafile, _ = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.cloud_path)

        # Download for first time.
        new_datafile.download()

        # Check that a new file isn't downloaded the second time.
        with patch("tempfile.NamedTemporaryFile") as temporary_file_mock:
            new_datafile.download()
            temporary_file_mock.assert_not_called()

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
        datafile, contents = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.cloud_path)

        with new_datafile.open() as f:
            self.assertEqual(f.read(), contents)

    def test_open_with_writing_to_cloud_file(self):
        """Test that a cloud datafile can be opened for writing and that both the remote and local copies are updated."""
        datafile, original_contents = self.create_datafile_in_cloud()
        new_datafile = Datafile(datafile.cloud_path)

        new_file_contents = "nanana"

        with new_datafile.open("w") as f:
            f.write(new_file_contents)

            # Check that the cloud file isn't updated until the context manager is closed.
            self.assertEqual(GoogleCloudStorageClient().download_as_string(datafile.cloud_path), original_contents)

        # Check that the cloud file has now been updated.
        self.assertEqual(GoogleCloudStorageClient().download_as_string(datafile.cloud_path), new_file_contents)

        # Check that the local copy has been updated.
        with new_datafile.open() as f:
            self.assertEqual(f.read(), new_file_contents)

    def test_deserialise(self):
        """Test that a Datafile can be deserialised faithfully."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("hello")

        datafile = Datafile(path=temporary_file.name)
        serialised_datafile = datafile.to_primitive()
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
        datafile = Datafile(path=filename)
        serialised_datafile = datafile.to_primitive()

        pathable = Pathable(path=os.path.join(os.sep, "an", "absolute", "path"))
        deserialised_datafile = Datafile.deserialise(serialised_datafile, path_from=pathable)

        self.assertEqual(datafile.id, deserialised_datafile.id)
        self.assertEqual(deserialised_datafile.absolute_path, os.path.join(pathable.absolute_path, filename))

        os.remove(temporary_file.name)

    def test_deserialise_ignores_path_from_if_path_is_absolute(self):
        """Test that Datafile.deserialise ignores the path_from parameter if the datafile's path is absolute."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("hello")

        datafile = Datafile(path=temporary_file.name)
        serialised_datafile = datafile.to_primitive()

        pathable = Pathable(path=os.path.join(os.sep, "an", "absolute", "path"))
        deserialised_datafile = Datafile.deserialise(serialised_datafile, path_from=pathable)

        self.assertEqual(datafile.id, deserialised_datafile.id)
        self.assertFalse(pathable.path in deserialised_datafile.path)
        self.assertEqual(deserialised_datafile.path, temporary_file.name)

    def test_posix_timestamp(self):
        """Test that the posix timestamp property works properly."""
        datafile = Datafile(path="hello.txt")
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
        datafile, original_content = self.create_datafile_in_cloud()
        new_contents = "Here is the new content."
        self.assertNotEqual(original_content, new_contents)

        with Datafile(datafile.cloud_path, mode="w") as (new_datafile, f):
            new_datafile.add_labels("blue")
            f.write(new_contents)

        # Check that the cloud metadata has been updated.
        re_downloaded_datafile = Datafile(datafile.cloud_path)
        self.assertTrue("blue" in re_downloaded_datafile.labels)

        # The file cache must be cleared so the modified cloud file is downloaded.
        re_downloaded_datafile.local_path = None

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

        with Datafile(path=path, mode="w") as (datafile, f):
            f.write('{"my": "data"}')

        data = GoogleCloudStorageClient().download_as_string(path)
        self.assertEqual(data, '{"my": "data"}')

    def test_reset_local_path_of_local_datafile_results_in_error(self):
        """Test that resetting the local path of a local-only datafile results in an error."""
        datafile = Datafile(path="blah.txt")

        with self.assertRaises(exceptions.CloudLocationNotSpecified):
            datafile.local_path = None

    def test_creating_local_datafile_and_then_uploading_to_cloud_does_not_use_temporary_local_file(self):
        """Test that a local datafile that is then uploaded to the cloud writes data to the same local path when
        using the `open` method context manager rather than to a temporary local path.
        """
        try:
            with Datafile(path="blah.txt", mode="w") as (datafile, f):
                f.write("blah\n")

            cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "blah.txt")
            datafile.to_cloud(cloud_path=cloud_path)

            self.assertEqual(datafile.local_path, os.path.abspath("blah.txt"))
            self.assertEqual(datafile.cloud_path, cloud_path)

            with datafile.open("a") as f:
                f.writelines(["shibiddy"])

            # Check data has been written to the local file specified when the datafile was created.
            with open("blah.txt") as f:
                self.assertEqual(f.readlines(), ["blah\n", "shibiddy"])

        finally:
            os.remove("blah.txt")
            os.remove(".octue")

    def test_setting_local_path_to_path_corresponding_to_existing_file_fails(self):
        """Ensure that a datafile's local path cannot be set to an existing file's path."""
        with tempfile.NamedTemporaryFile(delete=True) as temporary_file:
            datafile, *_ = self.create_datafile_in_cloud()

            with self.assertRaises(FileExistsError):
                datafile.local_path = temporary_file.name

    def test_local_path_can_be_set_after_creating_cloud_datafile_and_is_used_instead_of_temporary_local_file(self):
        """Test that the local path can be set after creating a cloud datafile and that this new local path is used
        instead of a temporary local file when writing data to the datafile. The local file must be up to date with the
        cloud file as well as any local changes.
        """
        try:
            datafile, original_contents = self.create_datafile_in_cloud()
            datafile.local_path = "blib.txt"

            self.assertEqual(datafile.cloud_path, storage.path.generate_gs_path(TEST_BUCKET_NAME, "cloud_file.txt"))
            self.assertEqual(datafile.local_path, os.path.abspath("blib.txt"))

            with datafile.open("a") as f:
                f.write("shibiddy")

            # Check that the original contents of the cloud file has been persisted to the new local path and new data
            # has gone to both.
            expected_contents = original_contents + "shibiddy"

            with open("blib.txt") as f:
                self.assertEqual(f.read(), expected_contents)

            self.assertEqual(GoogleCloudStorageClient().download_as_string(datafile.cloud_path), expected_contents)

        finally:
            os.remove("blib.txt")
            os.remove(".octue")

    def test_cloud_path_property(self):
        """Test that the cloud path property returns the expected value."""
        datafile = Datafile(path="gs://blah/no.txt", hypothetical=True)
        self.assertEqual(datafile.cloud_path, "gs://blah/no.txt")

    def test_setting_cloud_path_property(self):
        """Test that setting the cloud path property of a local datafile results in the local file's data being written
        to the cloud.
        """
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("hello")

        datafile = Datafile(path=temporary_file.name)
        self.assertIsNone(datafile.cloud_path)

        datafile.cloud_path = f"gs://{TEST_BUCKET_NAME}/my-file.dat"

        # Check that the local file's contents have been written to the cloud path.
        self.assertEqual(GoogleCloudStorageClient().download_as_string(datafile.cloud_path), "hello")

    def test_instantiating_local_file_with_cloud_path(self):
        """Test that a local datafile instantiated with a cloud path causes the local file to be uploaded to the cloud."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            temporary_file.write("blah")

        datafile = Datafile(path=temporary_file.name, cloud_path=f"gs://{TEST_BUCKET_NAME}/{temporary_file.name}")

        self.assertTrue(datafile.exists_locally)
        self.assertTrue(datafile.exists_in_cloud)

        # Check that the local file's contents have been written to the cloud path.
        self.assertEqual(GoogleCloudStorageClient().download_as_string(datafile.cloud_path), "blah")

    def test_instantiating_cloud_file_with_non_existent_local_path(self):
        """Test that a cloud datafile instantiated with a non-existent local path is kept in sync with the cloud object."""
        cloud_path = f"gs://{TEST_BUCKET_NAME}/cake/taste.txt"
        GoogleCloudStorageClient().upload_from_string("yum", cloud_path=cloud_path)

        with tempfile.TemporaryDirectory() as temporary_directory:

            datafile = Datafile(path=cloud_path, local_path=os.path.join(temporary_directory, "my-file.txt"))

            self.assertTrue(datafile.exists_locally)
            self.assertTrue(datafile.exists_in_cloud)

            with datafile.open("a") as f:
                f.write("yum")

            with open(datafile.local_path) as f:
                self.assertEqual(f.read(), "yumyum")

    def test_instantiating_cloud_file_with_existing_local_path(self):
        """Test that a cloud datafile instantiated with an existent local path is kept in sync with the cloud object and
        that any previous contents the file at the local path has is overwritten by the contents of the cloud object.
        """
        cloud_path = f"gs://{TEST_BUCKET_NAME}/cake/taste.txt"
        GoogleCloudStorageClient().upload_from_string("yum", cloud_path=cloud_path)

        with tempfile.TemporaryDirectory() as temporary_directory:
            local_path = os.path.join(temporary_directory, "my-file.txt")

            with open(local_path, "w") as f:
                f.write("blah")

            datafile = Datafile(path=cloud_path, local_path=local_path)

            self.assertTrue(datafile.exists_locally)
            self.assertTrue(datafile.exists_in_cloud)

            with datafile.open("a") as f:
                f.write("yum")

            with open(datafile.local_path) as f:
                self.assertEqual(f.read(), "yumyum")

    def test_bucket_name_and_path_in_bucket_properties(self):
        """Test the bucket_name and path_in_bucket properties work as expected for cloud and local datafiles."""
        datafile = Datafile(path="gs://my-bucket/directory/hello.txt", hypothetical=True)
        self.assertEqual(datafile.bucket_name, "my-bucket")
        self.assertEqual(datafile.path_in_bucket, "directory/hello.txt")

        datafile = Datafile(path="local_file.txt")
        self.assertIsNone(datafile.bucket_name)
        self.assertIsNone(datafile.path_in_bucket)

    def test_datafiles_with_space_in_name_can_be_uploaded_downloaded_serialized_and_deserialized(self):
        """Test that a datafile with a space in its name can be uploaded, downloaded, serialized, and deserialized."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            local_path = os.path.join(temporary_directory, "name with spaces.txt")

            with open(local_path, "w") as f:
                f.write("blah")

            datafile = Datafile(path=local_path)

            serialized_datafile = datafile.to_primitive()
            deserialized_datafile = Datafile.deserialise(serialized_datafile)
            self.assertEqual(deserialized_datafile.name, "name with spaces.txt")

            datafile.to_cloud(cloud_path=f"gs://{TEST_BUCKET_NAME}/name with spaces.txt")

        downloaded_datafile = Datafile(path=f"gs://{TEST_BUCKET_NAME}/name with spaces.txt")
        self.assertEqual(downloaded_datafile.name, "name with spaces.txt")

    def test_creating_new_hdf5_datafile(self):
        """Test that a new HDF5 datafile can be created and written to."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            datafile = Datafile(path=os.path.join(temporary_directory, "my-file.hdf5"))

            with datafile.open("w") as f:
                f["dataset"] = range(10)

            with h5py.File(datafile.local_path) as f:
                self.assertEqual(list(f["dataset"]), list(range(10)))

    def test_creating_datafile_from_existing_hdf5_file(self):
        """Test that a datafile can be created from an existing HDF5 file."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = os.path.join(temporary_directory, "my-file.hdf5")

            with h5py.File(path, "w") as f:
                f["dataset"] = range(10)

            with Datafile(path=path) as (datafile, f):
                self.assertEqual(list(f["dataset"]), list(range(10)))

    def test_uploading_hdf5_datafile_to_cloud(self):
        """Test that an HDF5 file can be uploaded to the cloud."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            datafile = Datafile(path=os.path.join(temporary_directory, "my-file.hdf5"))

            with datafile.open("w") as f:
                f["dataset"] = range(10)

            datafile.to_cloud(cloud_path=f"gs://{TEST_BUCKET_NAME}/my-file.hdf5")

            download_path = os.path.join(temporary_directory, "downloaded-file.hdf5")

            GoogleCloudStorageClient().download_to_file(local_path=download_path, cloud_path=datafile.cloud_path)

            with h5py.File(download_path) as f:
                self.assertEqual(list(f["dataset"]), list(range(10)))

    def test_error_raised_if_using_datafile_with_hdf5_file_without_h5py_package_available(self):
        """Test that trying to open an HDF5 datafile when the `h5py` dependency is not available results in an error."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            datafile = Datafile(path=os.path.join(temporary_directory, "my-file.hdf5"))

            with patch("pkg_resources.get_distribution", side_effect=pkg_resources.DistributionNotFound()):
                with self.assertRaises(ImportError):
                    with datafile.open("w") as f:
                        f["dataset"] = range(10)

    def test_metadata_is_saved_locally_when_in_write_mode_and_is_loaded_on_new_instantiation(self):
        """Test that metadata for a local datafile is saved locally if in write mode and loaded in new instantiations of
        the same file.
        """
        new_labels = {"yes", "no", "maybe"}

        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with Datafile(path=temporary_file.name, mode="w") as (datafile, f):
                datafile.labels = new_labels

            reloaded_datafile = Datafile(path=temporary_file.name)
            self.assertEqual(reloaded_datafile.labels, new_labels)
            self.assertEqual(reloaded_datafile.id, datafile.id)
            self.assertEqual(reloaded_datafile.hash_value, datafile.hash_value)

    def test_local_metadata_is_not_saved_locally_if_changed_in_read_mode(self):
        """Test that local metadata for a datafile is not saved if changed in read mode."""
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with Datafile(path=temporary_file.name, mode="r") as (datafile, f):
                datafile.labels = {"yes", "no", "maybe"}

            reloaded_datafile = Datafile(path=temporary_file.name)
            self.assertEqual(reloaded_datafile.labels, LabelSet())
            self.assertEqual(reloaded_datafile.hash_value, datafile.hash_value)

    def test_local_metadata_is_updated_if_changed_in_write_mode(self):
        """Test that local metadata for a datafile is updated if the datafile's metadata is updated in write mode."""
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with Datafile(path=temporary_file.name, mode="w") as (datafile, f):
                datafile.labels = {"yes", "no", "maybe"}

            self.assertEqual(datafile.labels, {"yes", "no", "maybe"})
            self.assertEqual(datafile.tags, {})

            # Change the labels and tags.
            with Datafile(path=temporary_file.name, mode="w") as (reloaded_datafile, f):
                reloaded_datafile.labels = {"blah", "nah"}
                reloaded_datafile.tags = {"my_tag": "hello"}

            # Check that the local metadata for the file is updated.
            datafile_reloaded_again = Datafile(path=temporary_file.name)
            self.assertEqual(datafile_reloaded_again.labels, {"blah", "nah"})
            self.assertEqual(datafile_reloaded_again.tags, {"my_tag": "hello"})
            self.assertEqual(datafile_reloaded_again.id, datafile.id)
            self.assertEqual(datafile_reloaded_again.hash_value, datafile.hash_value)

    def test_pickle_datafile(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            with Datafile(path=os.path.join(temporary_directory, "my-file.dat"), mode="w") as (datafile, f):
                f.write("hello")

        picked_datafile = pickle.dumps(datafile)
        unpickled_datafile = pickle.loads(picked_datafile)
