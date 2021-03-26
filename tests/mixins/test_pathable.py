import os

from octue.exceptions import InvalidInputException
from octue.mixins import MixinBase, Pathable
from ..base import BaseTestCase


class MyPathable(Pathable, MixinBase):
    pass


class PathableTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments, with default paths at the current working directory"""
        resource = MyPathable()
        self.assertEqual(os.getcwd(), resource.absolute_path)
        self.assertEqual(".", resource.path_relative_to())
        self.assertEqual(".", resource.path)

    def test_paths_chain(self):
        """Ensures that pathable resources daisychain their paths"""
        owner = MyPathable()
        owned = MyPathable(path_from=owner, path="owned")
        owned_owned = MyPathable(path_from=owned, path="owned_owned")
        self.assertEqual(os.path.join(os.getcwd(), "owned", "owned_owned"), owned_owned.absolute_path)

    def test_paths_chain_dynamic(self):
        """Ensures that daisychaining updates if path in the chain changes

        This enables us to initialise (for example) datasets from manifests, where all the paths are given relative to
        the dataset, then alter the path further up the tree to where a directory actually is on the system.
        """
        owner = MyPathable()
        owned = MyPathable(path_from=owner, path="owned")
        self.assertEqual(os.path.join(os.getcwd(), "owned"), owned.absolute_path)
        owner._path = "dynamic"
        self.assertEqual(os.path.join(os.getcwd(), "dynamic", "owned"), owned.absolute_path)

    def test_paths_chain_with_missing_values(self):
        """Ensures that pathable resources chain even if a part of the chain doesn't have a path"""

        # Owner is in the current working directory
        owner = MyPathable(path="owner")
        owned = MyPathable(path_from=owner)  # This resource doesn't have a path property
        owned_owned = MyPathable(path_from=owned, path="owned_owned")
        self.assertEqual(os.path.join(os.getcwd(), "owner", "owned_owned"), owned_owned.absolute_path)

    def test_paths_relative(self):
        """Ensures that pathable resources have a relative path (by default relative to current working directory)"""
        owner = MyPathable(path="owner")
        owned = MyPathable(path_from=owner, path="owned")
        self.assertEqual(os.path.join("owner", "owned"), owned.path_relative_to())

    def test_paths_relative_to_base(self):
        """Ensures that pathable resources have a relative path that is relative to base_path if given"""
        # Check it works for a single depth
        owner1 = MyPathable(path="owner")
        owned1 = MyPathable(path_from=owner1, path="owned")
        self.assertEqual("owned", owned1.path_relative_to(owner1))

        # Check it works at from several depths
        owner2 = MyPathable(path="owner")
        owned2 = MyPathable(path_from=owner2, path="owned")
        owned_owned2 = MyPathable(path_from=owned2, path="owned_owned")
        self.assertEqual(os.path.join("owned", "owned_owned"), owned_owned2.path_relative_to(owner2))

    def test_invalid_path_from(self):
        """Ensures that exceptions are correctly raised when the *_from objects are not Pathables"""

        class NotPathable:
            pass

        with self.assertRaises(InvalidInputException):
            MyPathable(path="owner", path_from=NotPathable())

    def test_valid_absolute_path_without_from_path(self):
        """Ensures that an absolute path can be set if no from_path object is present"""
        owner1 = MyPathable(path=f"{os.sep}owner")
        self.assertEqual(f"{os.sep}owner", owner1.absolute_path)

    def test_invalid_absolute_path_with_from_path(self):
        """Ensures that pathable resources have a relative path that is relative to base_path if given"""
        # Check it works for a single depth
        owner1 = MyPathable(path="owner")
        with self.assertRaises(InvalidInputException):
            MyPathable(path_from=owner1, path="/owned")

    def test_with_google_cloud_storage_blob(self):
        """Test that paths in Google Cloud storage buckets can be represented as a Pathable."""
        path = "gs://my-bucket/file/in/bucket.json"
        pathable = Pathable(path=path)
        self.assertEqual(pathable.path, "gs://my-bucket/file/in/bucket.json")
        self.assertEqual(pathable.absolute_path, "gs://my-bucket/file/in/bucket.json")
        self.assertEqual(pathable.path_relative_to(), "my-bucket/file/in/bucket.json")

    def test_google_cloud_storage_blob_relative_path(self):
        """Test that paths in Google Cloud storage buckets can be represented as a Pathable."""
        path = "gs://my-bucket/file/in/bucket.json"
        pathable = Pathable(path=path)
        self.assertEqual(pathable.path_relative_to("my-bucket/file"), "in/bucket.json")
        self.assertEqual(pathable.path_relative_to("my-bucket/file/hello"), "../in/bucket.json")
