from octue.cloud import storage
from tests.base import BaseTestCase


class TestStorage(BaseTestCase):
    def test_join(self):
        """Test that paths can be joined correctly."""
        self.assertEqual(storage.path.join(), "")
        self.assertEqual(storage.path.join("a", "b", "c"), "a/b/c")
        self.assertEqual(storage.path.join("/a", "b", "c"), "/a/b/c")
        self.assertEqual(storage.path.join("", "nah", "blah"), "nah/blah")
        self.assertEqual(storage.path.join("", "nah", "", "blah"), "nah/blah")
        self.assertEqual(storage.path.join("a", "b", "/c"), "/c")
        self.assertEqual(storage.path.join("a", "b", "/c", "d"), "/c/d")

    def test_generate_gs_path(self):
        """Test that the `gs://` path can be generated correctly."""
        self.assertEqual(storage.path.generate_gs_path("my-bucket"), "gs://my-bucket")
        self.assertEqual(storage.path.generate_gs_path("my-bucket", "nah", "blah"), "gs://my-bucket/nah/blah")
        self.assertEqual(storage.path.generate_gs_path("my-bucket", "/nah", "blah"), "gs://my-bucket/nah/blah")

    def test_split_bucket_name_from_gs_path(self):
        """Test that the bucket name can be split from the path in a gs path."""
        self.assertEqual(
            storage.path.split_bucket_name_from_gs_path("gs://my-bucket/path/file.txt"), ("my-bucket", "path/file.txt")
        )
        self.assertEqual(storage.path.split_bucket_name_from_gs_path("gs://my-bucket"), ("my-bucket", ""))

    def test_strip_protocol_from_path(self):
        """Test that the `gs://` protocol can be stripped from a path."""
        self.assertEqual(storage.path.strip_protocol_from_path("gs://my-bucket"), "my-bucket")
        self.assertEqual(storage.path.strip_protocol_from_path("gs://my-bucket/"), "my-bucket/")
        self.assertEqual(storage.path.strip_protocol_from_path("gs://my-bucket/a/b/c"), "my-bucket/a/b/c")

    def test_relpath_with_gs_protocol(self):
        """Test that relative paths are calculated correctly for cloud paths."""
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="gs://my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="gs://my-bucket/"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="gs://my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="gs://my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="gs://my-bucket/a/b/c/"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="gs://my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="gs://my-bucket/a/d"), "../b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/d", start="gs://my-bucket/a/b/c"), "../../d")

    def test_relpath_with_gs_protocol_for_path(self):
        """Test that relative paths are calculated correctly for cloud paths."""
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="gs://my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="gs://my-bucket/"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="gs://my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="gs://my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="gs://my-bucket/a/b/c/"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="gs://my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="gs://my-bucket/a/d"), "../b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/d", start="gs://my-bucket/a/b/c"), "../../d")

    def test_relpath_without_gs_protocol_for_start(self):
        """Test that relative paths are calculated correctly for cloud paths."""
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="my-bucket/"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c", start="my-bucket/a/b/c/"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/b/c/", start="my-bucket/a/d"), "../b/c")
        self.assertEqual(storage.path.relpath(path="gs://my-bucket/a/d", start="my-bucket/a/b/c"), "../../d")

    def test_relpath_without_gs_protocol(self):
        """Test that relative paths are calculated correctly for cloud paths."""
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="my-bucket/"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="my-bucket"), "a/b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c", start="my-bucket/a/b/c/"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="my-bucket/a/b/c"), ".")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/b/c/", start="my-bucket/a/d"), "../b/c")
        self.assertEqual(storage.path.relpath(path="my-bucket/a/d", start="my-bucket/a/b/c"), "../../d")

    def test_split(self):
        """Test that cloud paths can be split properly."""
        self.assertEqual(storage.path.split("my-bucket/a/b/c.txt"), ("my-bucket/a/b", "c.txt"))

    def test_dirname(self):
        """Test that the name of the directory of the given path can be found."""
        self.assertEqual(storage.path.dirname("a/b/c"), "a/b")
        self.assertEqual(storage.path.dirname("a/b/c", name_only=True), "b")
        self.assertEqual(storage.path.dirname("/a/b/c"), "/a/b")
        self.assertEqual(storage.path.dirname("/a/b/c", name_only=True), "b")
        self.assertEqual(storage.path.dirname("a"), "")
        self.assertEqual(storage.path.dirname("/a"), "/")
