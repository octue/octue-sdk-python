from octue import exceptions
from octue.mixins import Taggable
from .base import BaseTestCase


class TaggableTestCase(BaseTestCase):
    """ Test case that ensures the methods of the Taggable class work correctly
    """

    def test_instantiates(self):
        """ Ensures the class instantiates without arguments
        """
        Taggable()

    def test_instantiates_with_tags(self):
        """ Ensures datafile inherits correctly from the Taggable class and passes arguments through
        """
        tgd = Taggable(tags="")
        self.assertEqual("", str(tgd.tags))
        tgd = Taggable(tags=None)
        self.assertEqual("", str(tgd.tags))
        tgd = Taggable(tags="a b c")
        self.assertEqual("a b c", str(tgd.tags))
        with self.assertRaises(exceptions.InvalidTagException):
            Taggable(tags=":a b c")

    def test_valid_tags(self):
        """ Ensures valid tags do not raise an error
        """
        tgd = Taggable()
        tgd.add_tags("a-valid-tag")
        tgd.add_tags("a:tag")
        tgd.add_tags("a:-tag")  # <--- yes, this is valid deliberately as it allows people to do negation
        tgd.add_tags("a1829tag")
        tgd.add_tags("1829")
        tgd.add_tags("number:1829")
        tgd.add_tags("multiple:discriminators:used")
        self.assertEqual(
            str(tgd.tags), "a-valid-tag a:tag a:-tag a1829tag 1829 number:1829 multiple:discriminators:used"
        )

    def test_invalid_tags(self):
        """ Ensures invalid tags raise an error
        """

        tgd = Taggable()
        with self.assertRaises(exceptions.InvalidTagException):
            tgd.add_tags("-bah")

        with self.assertRaises(exceptions.InvalidTagException):
            tgd.add_tags("humbug:")

        with self.assertRaises(exceptions.InvalidTagException):
            tgd.add_tags("SHOUTY")

        with self.assertRaises(exceptions.InvalidTagException):
            tgd.add_tags(r"back\slashy")

        with self.assertRaises(exceptions.InvalidTagException):
            tgd.add_tags({"not-a": "string"})

    def test_mixture_valid_invalid(self):
        """ Ensures that adding a variety of tags, some of which are invalid, doesn't partially add them to the object
        """
        tgd = Taggable()
        tgd.add_tags("first-valid-should-be-added")
        try:
            tgd.add_tags("second-valid-should-not-be-added-because", "-the-third-is-invalid:")

        except exceptions.InvalidTagException:
            pass

        self.assertEqual("first-valid-should-be-added", str(tgd.tags))

    def test_serialises_to_string(self):
        """ Ensures that adding a variety of tags, some of which are invalid, doesn't partially add them to the object
        """
        tgd = Taggable(tags="a b")
        self.assertEqual("a b", tgd.tags.serialise())
