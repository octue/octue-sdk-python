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
        self.assertEqual(MyTaggable().tags, TagDict())

    def test_fails_to_instantiates_with_non_iterable(self):
        """Test that instantiation with a non-iterable fails."""

        class NoIter:
            pass

        with self.assertRaises(TypeError):
            MyTaggable(tags=NoIter())

    def test_instantiates_with_dict(self):
        """Test instantiation with a dictionary."""
        tags = {"height": 9, "width": 8.7, "depth": 100}
        taggable = MyTaggable(tags)
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

        taggable.add_tags({"a-valid-tag": "blah"})
        taggable.add_tags({"a:tag": "blah"})
        taggable.add_tags({"a:-tag": "blah"})  # <--- yes, this is valid deliberately as it allows people to do negation
        taggable.add_tags({"a1829tag": "blah"})
        taggable.add_tags({"multiple:discriminators:used": "blah"})
        taggable.add_tags({"1829": "blah", "number:1829": "blah"})  # Add multiple tags at once.

        self.assertEqual(
            taggable.tags,
            {
                "a-valid-tag": "blah",
                "a:tag": "blah",
                "a:-tag": "blah",
                "a1829tag": "blah",
                "1829": "blah",
                "number:1829": "blah",
                "multiple:discriminators:used": "blah",
            },
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
        taggable.add_tags({"first-valid-should-be-added": "hello"})

        with self.assertRaises(exceptions.InvalidTagException):
            taggable.add_tags({"second-valid-should-not-be-added-because": 1, "-the-third-is-invalid:": 2})

        self.assertEqual(taggable.tags, {"first-valid-should-be-added": "hello"})
