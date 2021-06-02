from unittest import TestCase

from octue import exceptions
from octue.resources.tag import TagDict


class TestTagDict(TestCase):
    def test_instantiate_from_dict(self):
        """Test that a TagDict can be instantiated from a dictionary."""
        tag_dict = TagDict({"a": 1, "b": 2})
        self.assertEqual(tag_dict, {"a": 1, "b": 2})

    def test_instantiate_from_kwargs(self):
        """Test that a TagDict can be instantiated from kwargs."""
        tag_dict = TagDict(**{"a": 1, "b": 2})
        self.assertEqual(tag_dict, {"a": 1, "b": 2})

    def test_instantiation_fails_if_tag_name_fails_validation(self):
        """Test that TagDict instantiation fails if any keys don't conform to the tag name pattern."""
        with self.assertRaises(exceptions.InvalidTagException):
            TagDict({".blah.": "blue"})

    def test_update_fails_if_tag_name_fails_validation(self):
        """Test that updating fails if any keys don't conform to the tag name pattern."""
        tag_dict = TagDict({"a": 1, "b": 2})

        with self.assertRaises(exceptions.InvalidTagException):
            tag_dict.update({"@": 3, "d": 4})

        self.assertEqual(tag_dict, {"a": 1, "b": 2})

    def test_update(self):
        """Test that TagDicts can be updated with tags with valid names."""
        tag_dict = TagDict({"a": 1, "b": 2})
        tag_dict.update({"c": 3, "d": 4})
        self.assertEqual(tag_dict, {"a": 1, "b": 2, "c": 3, "d": 4})

    def test_setitem_fails_if_tag_name_fails_validation(self):
        """Test that setting an item on a TagDict fails if the name fails validation."""
        tag_dict = TagDict()

        with self.assertRaises(exceptions.InvalidTagException):
            tag_dict["@@@"] = 9

    def test_setitem(self):
        """Test setting an item on a TagDict."""
        tag_dict = TagDict()
        tag_dict["hello"] = 9
        self.assertEqual(tag_dict, {"hello": 9})

    def test_equality_to_dict(self):
        """Test that TagDicts compare equal to dictionaries with the same contents."""
        tag_dict = TagDict({"a": 1, "b": 2})
        self.assertEqual(tag_dict, {"a": 1, "b": 2})
