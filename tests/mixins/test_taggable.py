from octue import exceptions
from octue.mixins import MixinBase, Taggable
from octue.mixins.taggable import TagGroup
from ..base import BaseTestCase


class MyTaggable(Taggable, MixinBase):
    pass


class TaggableTestCase(BaseTestCase):
    def test_instantiates(self):
        """ Ensures the class instantiates without arguments
        """
        Taggable()

    def test_instantiates_with_tags(self):
        """ Ensures datafile inherits correctly from the Taggable class and passes arguments through
        """
        taggable = MyTaggable(tags="")
        self.assertEqual(len(taggable.tags), 0)

        taggable = MyTaggable(tags=None)
        self.assertEqual(len(taggable.tags), 0)

        taggable = MyTaggable(tags="a b c")
        self.assertEqual(set(taggable.tags), {"a", "b", "c"})

        with self.assertRaises(exceptions.InvalidTagException):
            MyTaggable(tags=":a b c")

    def test_instantiates_with_tag_group(self):
        """ Ensures datafile inherits correctly from the Taggable class and passes arguments through
        """
        taggable_1 = MyTaggable(tags="")
        self.assertIsInstance(taggable_1.tags, TagGroup)
        taggable_2 = MyTaggable(tags=taggable_1.tags)
        self.assertFalse(taggable_1 is taggable_2)

    def test_fails_to_instantiates_with_non_iterable(self):
        """ Ensures datafile inherits correctly from the Taggable class and passes arguments through
        """

        class NoIter:
            pass

        with self.assertRaises(exceptions.InvalidTagException) as error:
            MyTaggable(tags=NoIter())

        self.assertIn(
            "Tags must be expressed as a whitespace-delimited string or an iterable of strings", error.exception.args[0]
        )

    def test_reset_tags(self):
        """ Ensures datafile inherits correctly from the Taggable class and passes arguments through
        """
        taggable = MyTaggable(tags="a b")
        taggable.tags = "b c"
        self.assertEqual(set(taggable.tags), {"b", "c"})

    def test_valid_tags(self):
        """ Ensures valid tags do not raise an error
        """
        tgd = MyTaggable()
        tgd.add_tags("a-valid-tag")
        tgd.add_tags("a:tag")
        tgd.add_tags("a:-tag")  # <--- yes, this is valid deliberately as it allows people to do negation
        tgd.add_tags("a1829tag")
        tgd.add_tags("1829")
        tgd.add_tags("number:1829")
        tgd.add_tags("multiple:discriminators:used")
        self.assertEqual(
            set(tgd.tags),
            {"a-valid-tag", "a:tag", "a:-tag", "a1829tag", "1829", "number:1829", "multiple:discriminators:used"},
        )

    def test_invalid_tags(self):
        """ Ensures invalid tags raise an error
        """
        tgd = MyTaggable()

        for tag in "-bah", "humbug:", "SHOUTY", r"back\slashy", {"not-a": "string"}:
            with self.assertRaises(exceptions.InvalidTagException):
                tgd.add_tags(tag)

    def test_mixture_valid_invalid(self):
        """ Ensures that adding a variety of tags, some of which are invalid, doesn't partially add them to the object
        """
        taggable = MyTaggable()
        taggable.add_tags("first-valid-should-be-added")
        try:
            taggable.add_tags("second-valid-should-not-be-added-because", "-the-third-is-invalid:")

        except exceptions.InvalidTagException:
            pass

        self.assertEqual({"first-valid-should-be-added"}, set(taggable.tags))


class TestTagGroup(BaseTestCase):
    TAG_GROUP = TagGroup(tags="a b:c d:e:f")

    def test_equality(self):
        """ Ensure two TagGroups with the same tags compare equal. """
        self.assertTrue(self.TAG_GROUP == TagGroup(tags="a b:c d:e:f"))

    def test_inequality(self):
        """ Ensure two TagGroups with different tags compare unequal. """
        self.assertTrue(self.TAG_GROUP != TagGroup(tags="a"))

    def test_iterating_over(self):
        """ Ensure a TagGroup can be iterated over. """
        self.assertEqual(set(self.TAG_GROUP), {"a", "b:c", "d:e:f"})

    def test_has_tag(self):
        """ Ensure we can check that a TagGroup has a certain tag. """
        self.assertTrue(self.TAG_GROUP.has_tag("d:e:f"))

    def test_does_not_have_tag(self):
        """ Ensure we can check that a TagGroup does not have a certain tag. """
        self.assertFalse(self.TAG_GROUP.has_tag("hello"))

    def test_has_tag_only_matches_full_tags(self):
        """ Test that the has_tag method only matches full tags (i.e. that it doesn't match subtags or parts of tags."""
        for tag in "a", "b:c", "d:e:f":
            self.assertTrue(self.TAG_GROUP.has_tag(tag))

        for tag in "b", "c", "d", "e", "f":
            self.assertFalse(self.TAG_GROUP.has_tag(tag))

    def test_yield_subtags(self):
        """ Test that subtags can be yielded from tags, including the main tags themselves. """
        self.assertEqual(set(self.TAG_GROUP._yield_subtags()), {"a", "b", "c", "d", "e", "f"})

    def test_get_subtags(self):
        """ Test subtags can be accessed as a new TagGroup. """
        self.assertEqual(TagGroup("meta:sys2:3456 blah").get_subtags(), TagGroup("meta sys2 3456 blah"))

    def test_starts_with_with_no_subtags(self):
        """ Ensure starts_with doesn't check starts of subtags by default. """
        for tag in "a", "b", "d":
            self.assertTrue(self.TAG_GROUP.starts_with(tag))

        for tag in "c", "e", "f":
            self.assertFalse(self.TAG_GROUP.starts_with(tag))

    def test_starts_with_with_subtags(self):
        """ Ensure starts_with checks starts of subtags if asked. """
        for tag in "a", "b", "c", "d", "e", "f":
            self.assertTrue(self.TAG_GROUP.starts_with(tag, consider_separate_subtags=True))

    def test_endsswith_with_no_subtags(self):
        """ Ensure ends_with doesn't check ends of subtags by default. """
        for tag in "a", "c", "f":
            self.assertTrue(self.TAG_GROUP.ends_with(tag))

        for tag in "b", "d", "e":
            self.assertFalse(self.TAG_GROUP.ends_with(tag))

    def test_ends_with_with_subtags(self):
        """ Ensure ends_with checks ends of subtags if asked. """
        for tag in "a", "b", "c", "d", "e", "f":
            self.assertTrue(self.TAG_GROUP.ends_with(tag, consider_separate_subtags=True))

    def test_contains_searches_for_tags_and_subtags(self):
        """ Ensure tags and subtags can be searched for. """
        for tag in "a", "b", "d":
            self.assertTrue(self.TAG_GROUP.contains(tag))

        for subtag in "c", "e", "f":
            self.assertTrue(self.TAG_GROUP.contains(subtag))

    def test_filter(self):
        """ Test that tag groups can be filtered. """
        tag_group = TagGroup(tags="tag1 tag2 meta:sys1:1234 meta:sys2:3456 meta:sys2:55")
        self.assertEqual(
            tag_group.filter("starts_with", "meta"), TagGroup("meta:sys1:1234 meta:sys2:3456 meta:sys2:55")
        )

    def test_filter_chaining(self):
        """ Test that filters can be chained. """
        tag_group = TagGroup(tags="tag1 tag2 meta:sys1:1234 meta:sys2:3456 meta:sys2:55")

        filtered_tags_1 = tag_group.filter("starts_with", "meta")
        self.assertEqual(filtered_tags_1, TagGroup("meta:sys1:1234 meta:sys2:3456 meta:sys2:55"))

        filtered_tags_2 = filtered_tags_1.filter("contains", "sys2", consider_separate_subtags=True)
        self.assertEqual(filtered_tags_2, TagGroup("meta:sys2:3456 meta:sys2:55"))

        filtered_tags_3 = filtered_tags_1.filter("exact", "meta:sys2:55", consider_separate_subtags=True)
        self.assertEqual(filtered_tags_3, TagGroup("meta:sys2:55"))

    def test_serialise(self):
        """ Ensure that TagGroups are serialised to the string form of a list. """
        self.assertEqual(self.TAG_GROUP.serialise(), "['a', 'b:c', 'd:e:f']")

    def test_serialise_orders_tags(self):
        """ Ensure that TagGroups are serialised to the string form of a list. """
        tag_group = TagGroup("z hello a c:no")
        self.assertEqual(tag_group.serialise(), "['a', 'c:no', 'hello', 'z']")
