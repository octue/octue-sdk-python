from unittest import TestCase
from unittest.mock import Mock

from octue.utils.objects import get_nested_attribute, getattr_or_subscribe


class TestObjects(TestCase):
    def test_getattr_or_subscribe_with_dictionary(self):
        """Test that the getattr_or_subscribe function can get values from a dictionary."""
        self.assertEqual(getattr_or_subscribe(instance={"hello": "world"}, name="hello"), "world")

    def test_getattr_or_subscribe_with_object(self):
        """Test that the getattr_or_subscribe function can get attribute values from a class instance."""
        self.assertEqual(getattr_or_subscribe(instance=Mock(hello="world"), name="hello"), "world")

    def test_get_nested_attribute(self):
        """Test that nested attributes can be accessed."""
        inner_mock = Mock(b=3)
        outer_mock = Mock(a=inner_mock)
        self.assertEqual(get_nested_attribute(instance=outer_mock, nested_attribute_name="a.b"), 3)

    def test_get_nested_dictionary_attribute(self):
        """Test that nested attributes ending in a dictionary key can be accessed."""
        inner_mock = Mock(b={"hello": "world"})
        outer_mock = Mock(a=inner_mock)
        self.assertEqual(get_nested_attribute(instance=outer_mock, nested_attribute_name="a.b.hello"), "world")
