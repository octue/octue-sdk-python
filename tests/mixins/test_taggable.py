from octue import exceptions
from octue.mixins import MixinBase, Taggable
from octue.resources.tag import Tag, TagSet
from ..base import BaseTestCase


class MyTaggable(Taggable, MixinBase):
    pass


class TaggableTestCase(BaseTestCase):
    def test_instantiates(self):
        """Ensures the class instantiates without arguments"""
        Taggable()

    def test_instantiates_with_tags(self):
        """Ensures datafile inherits correctly from the Taggable class and passes arguments through"""
        taggable = MyTaggable(tags="")
        self.assertEqual(len(taggable.tags), 0)

        taggable = MyTaggable(tags=None)
        self.assertEqual(len(taggable.tags), 0)

        taggable = MyTaggable(tags="a b c")
        self.assertEqual(set(taggable.tags), {Tag("a"), Tag("b"), Tag("c")})

        with self.assertRaises(exceptions.InvalidTagException):
            MyTaggable(tags=":a b c")

    def test_instantiates_with_tag_set(self):
        """Ensures datafile inherits correctly from the Taggable class and passes arguments through"""
        taggable_1 = MyTaggable(tags="")
        self.assertIsInstance(taggable_1.tags, TagSet)
        taggable_2 = MyTaggable(tags=taggable_1.tags)
        self.assertFalse(taggable_1 is taggable_2)

    def test_fails_to_instantiates_with_non_iterable(self):
        """Ensures datafile inherits correctly from the Taggable class and passes arguments through"""

        class NoIter:
            pass

        with self.assertRaises(exceptions.InvalidTagException) as error:
            MyTaggable(tags=NoIter())

        self.assertIn(
            "Tags must be expressed as a whitespace-delimited string or an iterable of strings", error.exception.args[0]
        )

    def test_reset_tags(self):
        """Ensures datafile inherits correctly from the Taggable class and passes arguments through"""
        taggable = MyTaggable(tags="a b")
        taggable.tags = "b c"
        self.assertEqual(set(taggable.tags), {Tag("b"), Tag("c")})

    def test_valid_tags(self):
        """Ensures valid tags do not raise an error"""
        taggable = MyTaggable()
        taggable.add_tags("a-valid-tag")
        taggable.add_tags("a:tag")
        taggable.add_tags("a:-tag")  # <--- yes, this is valid deliberately as it allows people to do negation
        taggable.add_tags("a1829tag")
        taggable.add_tags("1829")
        taggable.add_tags("number:1829")
        taggable.add_tags("multiple:discriminators:used")
        self.assertEqual(
            set(taggable.tags),
            {
                Tag("a-valid-tag"),
                Tag("a:tag"),
                Tag("a:-tag"),
                Tag("a1829tag"),
                Tag("1829"),
                Tag("number:1829"),
                Tag("multiple:discriminators:used"),
            },
        )

    def test_invalid_tags(self):
        """Ensures invalid tags raise an error"""
        taggable = MyTaggable()

        for tag in "-bah", "humbug:", "SHOUTY", r"back\slashy", {"not-a": "string"}:
            with self.assertRaises(exceptions.InvalidTagException):
                taggable.add_tags(tag)

    def test_mixture_valid_invalid(self):
        """Ensures that adding a variety of tags, some of which are invalid, doesn't partially add them to the object"""
        taggable = MyTaggable()
        taggable.add_tags("first-valid-should-be-added")
        try:
            taggable.add_tags("second-valid-should-not-be-added-because", "-the-third-is-invalid:")

        except exceptions.InvalidTagException:
            pass

        self.assertEqual({Tag("first-valid-should-be-added")}, set(taggable.tags))
