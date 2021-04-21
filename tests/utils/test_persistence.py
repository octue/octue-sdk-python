import os
import tempfile
import time
import unittest

from octue.utils.persistence import calculate_disk_usage, get_oldest_file_in_directory


def _create_file_of_size(path, size):
    """Create a file at the given path of the given size in bytes.

    :param str path:
    :param int size:
    :return None:
    """
    with open(path, "wb") as f:
        f.truncate(size)


class TestCalculateDiskUsage(unittest.TestCase):
    def test_calculate_disk_usage_with_single_file(self):
        """Test that the disk usage of a single file is calculated correctly."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            pass

        _create_file_of_size(temporary_file.name, 1)
        self.assertEqual(calculate_disk_usage(temporary_file.name), 1)

    def test_with_filter_satisfied(self):
        """Test that files meeting a filter are included in the calculated size."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            dummy_file_path = os.path.join(temporary_directory, "dummy_file")
            _create_file_of_size(dummy_file_path, 1)

            self.assertEqual(
                calculate_disk_usage(dummy_file_path, filter=lambda path: os.path.split(path)[-1].startswith("dummy")),
                1,
            )

    def test_with_filter_unsatisfied(self):
        """Test that files not meeting a filter are not included in the calculated size."""
        with tempfile.NamedTemporaryFile("w", delete=False) as temporary_file:
            pass

        _create_file_of_size(temporary_file.name, 1)

        self.assertEqual(
            calculate_disk_usage(temporary_file.name, filter=lambda path: os.path.split(path)[-1].startswith("dummy")),
            0,
        )

    def test_calculate_disk_usage_with_shallow_directory(self):
        """Test that the disk usage of a directory with only files in is calculated correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                path = os.path.join(temporary_directory, f"dummy_file_{i}")
                _create_file_of_size(path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 5)

    def test_calculate_disk_usage_with_directory_of_directories(self):
        """Test that the disk usage of a directory of directories with files in is calculated correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                directory_path = os.path.join(temporary_directory, f"directory_{i}")
                os.mkdir(directory_path)

                for j in range(5):
                    file_path = os.path.join(directory_path, f"dummy_file_{j}")
                    _create_file_of_size(file_path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 25)

    def test_calculate_disk_usage_with_directory_of_directories_and_files(self):
        """Test that the disk usage of a directory of directories with files in and files next to the directories is
        calculated correctly.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                directory_path = os.path.join(temporary_directory, f"directory_{i}")
                os.mkdir(directory_path)

                adjacent_file_path = os.path.join(temporary_directory, f"adjacent_dummy_file_{i}")
                _create_file_of_size(adjacent_file_path, 2)

                for j in range(5):
                    file_path = os.path.join(directory_path, f"dummy_file_{j}")
                    _create_file_of_size(file_path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 35)


class TestGetOldestFileInDirectory(unittest.TestCase):
    def test_oldest_file_is_retrieved(self):
        """Test that the path of the oldest file in a directory is retrieved."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                _create_file_of_size(path=os.path.join(temporary_directory, f"file_{i}"), size=1)
                time.sleep(1)

            self.assertEqual(
                get_oldest_file_in_directory(temporary_directory), os.path.join(temporary_directory, "file_0")
            )

    def test_empty_directory_results_in_none(self):
        """Test that None is returned for an empty directory."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            self.assertEqual(get_oldest_file_in_directory(temporary_directory), None)

    def test_directories_ignored(self):
        """Test that directories are ignored."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                os.mkdir(os.path.join(temporary_directory, f"directory_{i}"))

            self.assertEqual(get_oldest_file_in_directory(temporary_directory), None)

    def test_only_files_satisfying_filter_are_considered(self):
        """Test that only files satisfying the filter are considered when getting the oldest file."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            _create_file_of_size(os.path.join(temporary_directory, "file_not_to_consider"), 1)
            _create_file_of_size(os.path.join(temporary_directory, "dummy_file"), 1)

            self.assertEqual(
                get_oldest_file_in_directory(
                    temporary_directory, filter=lambda path: os.path.split(path)[-1].startswith("dummy")
                ),
                os.path.join(temporary_directory, "dummy_file"),
            )
