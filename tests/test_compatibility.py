from octue.compatibility import is_compatible
from tests.base import BaseTestCase


class TestCompatibility(BaseTestCase):
    def test_compatible_if_versions_identical(self):
        """Test that identical versions are compatible."""
        self.assertTrue(is_compatible("0.3.7", "0.3.7"))

    def test_compatible_if_lower_version_above_latest_incompatible_version(self):
        """Test that two versions are compatible if the lower version is above the latest incompatible version."""
        self.assertTrue(is_compatible("0.27.1", "0.26.0"))

    def test_not_compatible_if_lower_version_is_latest_incompatible_version(self):
        """Test that two versions are incompatible if the lower version is the latest incompatible version."""
        self.assertFalse(is_compatible("0.27.1", "0.24.1"))

    def test_not_compatible_if_lower_version_below_latest_incompatible_version(self):
        """Test that two versions are incompatible if the lower version is lower than the latest incompatible version."""
        self.assertFalse(is_compatible("0.27.1", "0.0.1"))

    def test_not_compatible_if_no_data_on_higher_version(self):
        """Test that two versions are incompatible if no data is available on the latest incompatible version."""
        self.assertFalse(is_compatible("1000000.0.0", "0.26.0"))
