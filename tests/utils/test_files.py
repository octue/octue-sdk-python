import unittest

from octue.utils.files import RegisteredTemporaryDirectory, temporary_directories


class TestRegisteredTemporaryDirectory(unittest.TestCase):
    def test_is_registered(self):
        """Test that the directory is registered in the `temporary_directories` list."""
        file = RegisteredTemporaryDirectory()
        self.assertIn(file, temporary_directories)
