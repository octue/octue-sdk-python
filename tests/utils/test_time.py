from datetime import datetime
from unittest import TestCase

from octue.utils.time import convert_from_posix_time, convert_to_posix_time


class TestTime(TestCase):
    def test_convert_to_posix_time(self):
        """Test that datetime instances can be converted to posix time."""
        self.assertEqual(convert_to_posix_time(datetime(1970, 1, 1)), 0)
        self.assertEqual(convert_to_posix_time(datetime(2000, 1, 1)), 946684800)
        self.assertEqual(convert_to_posix_time(datetime(1940, 1, 1)), -946771200)

    def test_convert_from_posix_time(self):
        """Test that posix timestamps can be converted to datetime instances."""
        self.assertEqual(convert_from_posix_time(0), datetime(1970, 1, 1))
        self.assertEqual(convert_from_posix_time(946684800), datetime(2000, 1, 1))
        self.assertEqual(convert_from_posix_time(-946771200), datetime(1940, 1, 1))
