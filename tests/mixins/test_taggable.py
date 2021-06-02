from octue import exceptions
from octue.mixins import Taggable
from octue.resources.tag import TagDict
from ..base import BaseTestCase


class MyTaggable(Taggable):
    pass


class TaggableTestCase(BaseTestCase):
    def test_instantiates(self):
        """Ensures the class instantiates without arguments."""
        taggable = Taggable()
        self.assertEqual(taggable.tags, {})

    def test_instantiating_with_no_tags(self):
        """Test that instantiating a Taggable with no tags results in an empty TagDict on the tags attribute."""
        self.assertEqual(MyTaggable().tags, TagDict())

    def test_fails_to_instantiates_with_non_iterable(self):
        """Test that instantiation with a non-iterable fails."""

        class NoIter:
            pass

        with self.assertRaises(TypeError):
            MyTaggable(tags=NoIter())

    def test_instantiates_with_dict(self):
        """Test instantiation with a dictionary works."""
        tags = {"height": 9, "width": 8.7, "depth": 100}
        taggable = MyTaggable(tags=tags)
        self.assertEqual(taggable.tags, tags)

    def test_instantiates_with_tag_dict(self):
        """Test instantiation with a TagDict."""
        taggable_1 = MyTaggable(tags={"a": 2})
        self.assertIsInstance(taggable_1.tags, TagDict)
        taggable_2 = MyTaggable(tags=taggable_1.tags)
        self.assertFalse(taggable_1 is taggable_2)

    def test_setting_tags_overwrites_previous_tags(self):
        """Ensure tags can be overwritten with new ones."""
        taggable = MyTaggable(tags={"a": 1, "b": 2})
        taggable.tags = {"c": 3, "d": 4}
        self.assertEqual(taggable.tags, {"c": 3, "d": 4})

    def test_add_valid_tags(self):
        """Ensures adding valid tags works."""
        taggable = MyTaggable()

        taggable.add_tags({"a_valid_tag": "blah"})
        taggable.add_tags({"a1829tag": "blah"})
        taggable.add_tags({"1829": "blah", "number_1829": "blah"})  # Add multiple tags at once.

        self.assertEqual(
            taggable.tags,
            {"a_valid_tag": "blah", "a1829tag": "blah", "1829": "blah", "number_1829": "blah"},
        )

    def test_add_tags_via_kwargs(self):
        """Test tags can be added via kwargs."""
        taggable = MyTaggable()
        taggable.add_tags(hello="blib", hi="glib")
        self.assertEqual(taggable.tags, {"hello": "blib", "hi": "glib"})

    def test_adding_mixture_of_valid_and_invalid_tags_fails_completely(self):
        """Ensure that adding a variety of tags, some of which are invalid, doesn't partially add the set including the
        invalid tags to the object.
        """
        taggable = MyTaggable()
        taggable.add_tags({"first_valid_should_be_added": "hello"})

        with self.assertRaises(exceptions.InvalidTagException):
            taggable.add_tags({"second_valid_should_not_be_added_because": 1, "_the_third_is_invalid:": 2})

        self.assertEqual(taggable.tags, {"first_valid_should_be_added": "hello"})
