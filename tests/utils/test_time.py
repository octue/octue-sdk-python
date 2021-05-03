from datetime import datetime
from unittest import TestCase

from octue.utils.time import convert_to_posix_time


class TestConvertToPosixTime(TestCase):
    def test_convert_to_posix_time(self):
        """Test that datetime instances can be converted to posix time."""
        self.assertEqual(convert_to_posix_time(datetime(1970, 1, 1)), 0)
        self.assertEqual(convert_to_posix_time(datetime(2000, 1, 1)), 946684800)
        self.assertEqual(convert_to_posix_time(datetime(1940, 1, 1)), -946771200)
