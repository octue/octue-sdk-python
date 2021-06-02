import json

from octue import exceptions
from octue.resources.filter_containers import FilterSet
from octue.resources.label import Label, LabelSet
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.encoders import OctueJSONEncoder
from tests.base import BaseTestCase


class TestLabel(BaseTestCase):
    def test_invalid_labels_cause_error(self):
        """Test that invalid labels cause an error to be raised."""
        for label in (
            ":a",
            "@",
            "a_b",
            "-bah",
            "humbug:",
            r"back\slashy",
            {"not-a": "string"},
            "/a",
            "a/",
            "blah:3.5.",
            "HELLO-WORLD",
            "Asia/Pacific",
        ):
            with self.assertRaises(exceptions.InvalidLabelException):
                Label(label)

    def test_valid_labels(self):
        """Test that valid labels instantiate as expected."""
        for label in "hello", "hello-world", "hello-world-goodbye", "blah-35":
            Label(label)

    def test_label_comparison(self):
        """ Test that labels can be alphabetically compared. """
        self.assertTrue(Label("a") < Label("b"))
        self.assertTrue(Label("b") > Label("a"))
        self.assertTrue(Label("a") != Label("b"))
        self.assertTrue(Label("a") == Label("a"))

    def test_label_comparison_with_strings(self):
        """ Test that labels can be alphabetically compared with strings in both directions. """
        self.assertTrue(Label("a") < "b")
        self.assertTrue(Label("b") > "a")
        self.assertTrue(Label("a") != "b")
        self.assertTrue(Label("a") == "a")
        self.assertTrue("b" > Label("a"))
        self.assertTrue("a" < Label("b"))
        self.assertTrue("b" != Label("a"))
        self.assertTrue("a" == Label("a"))

    def test_labels_compare_unequal_to_non_str_or_label_types(self):
        """ Test that comparing for equality a Label with a non-string-or-Label type returns False. """
        self.assertFalse(Label("a") == 1)
        self.assertTrue(Label("a") != 1)

    def test_contains(self):
        """ Test that labels can be checked for containment. """
        self.assertIn("e", Label("hello"))

    def test_starts_with(self):
        """ Test that the start of a label can be checked. """
        self.assertTrue(Label("hello").startswith("h"))
        self.assertFalse(Label("hello").startswith("e"))

    def test_ends_with(self):
        """ Test that the end of a label can be checked. """
        self.assertTrue(Label("hello").endswith("o"))
        self.assertFalse(Label("hello").endswith("e"))


class TestLabelSet(BaseTestCase):
    LABEL_SET = LabelSet(labels="a b-c d-e-f")

    def test_instantiation_from_space_delimited_string(self):
        """ Test that a LabelSet can be instantiated from a space-delimited string of label names."""
        label_set = LabelSet(labels="a b-c d-e-f")
        self.assertEqual(label_set, self.LABEL_SET)

    def test_instantiation_from_iterable_of_strings(self):
        """ Test that a LabelSet can be instantiated from an iterable of strings."""
        label_set = LabelSet(labels=["a", "b-c", "d-e-f"])
        self.assertEqual(label_set, self.LABEL_SET)

    def test_instantiation_from_iterable_of_labels(self):
        """ Test that a LabelSet can be instantiated from an iterable of labels."""
        label_set = LabelSet(labels=[Label("a"), Label("b-c"), Label("d-e-f")])
        self.assertEqual(label_set, self.LABEL_SET)

    def test_instantiation_from_filter_set_of_strings(self):
        """ Test that a LabelSet can be instantiated from a FilterSet of strings."""
        label_set = LabelSet(labels=FilterSet({"a", "b-c", "d-e-f"}))
        self.assertEqual(label_set, self.LABEL_SET)

    def test_instantiation_from_filter_set_of_labels(self):
        """ Test that a LabelSet can be instantiated from a FilterSet of labels."""
        label_set = LabelSet(labels=FilterSet({Label("a"), Label("b-c"), Label("d-e-f")}))
        self.assertEqual(label_set, self.LABEL_SET)

    def test_instantiation_from_label_set(self):
        """ Test that a LabelSet can be instantiated from another LabelSet. """
        self.assertEqual(self.LABEL_SET, LabelSet(self.LABEL_SET))

    def test_equality(self):
        """ Ensure two LabelSets with the same labels compare equal. """
        self.assertTrue(self.LABEL_SET == LabelSet(labels="a b-c d-e-f"))
        self.assertTrue(self.LABEL_SET == {"a", "b-c", "d-e-f"})

    def test_inequality(self):
        """ Ensure two LabelSets with different labels compare unequal. """
        self.assertTrue(self.LABEL_SET != LabelSet(labels="a"))

    def test_non_label_sets_compare_unequal_to_label_sets(self):
        """ Ensure a LabelSet and a non-LabelSet compare unequal. """
        self.assertFalse(self.LABEL_SET == "a")
        self.assertTrue(self.LABEL_SET != "a")

    def test_iterating_over(self):
        """ Ensure a LabelSet can be iterated over. """
        self.assertEqual(set(self.LABEL_SET), {Label("a"), Label("b-c"), Label("d-e-f")})

    def test_contains_with_string(self):
        """ Ensure we can check that a LabelSet has a certain label using a string form. """
        self.assertTrue("d-e-f" in self.LABEL_SET)
        self.assertFalse("hello" in self.LABEL_SET)

    def test_contains_with_label(self):
        """ Ensure we can check that a LabelSet has a certain label. """
        self.assertTrue(Label("d-e-f") in self.LABEL_SET)
        self.assertFalse(Label("hello") in self.LABEL_SET)

    def test_contains_only_matches_full_labels(self):
        """ Test that the has_label method only matches full labels (i.e. that it doesn't match sublabels or parts of labels."""
        for label in "a", "b-c", "d-e-f":
            self.assertTrue(label in self.LABEL_SET)

        for label in "b", "c", "d", "e", "f":
            self.assertFalse(label in self.LABEL_SET)

    def test_add(self):
        """Test that the add method adds a valid label but raises an error for an invalid label."""
        label_set = LabelSet({"a", "b"})
        label_set.add("c")
        self.assertEqual(label_set, {"a", "b", "c"})

        with self.assertRaises(exceptions.InvalidLabelException):
            label_set.add("d_")

    def test_update(self):
        """Test that the update method adds valid labels but raises an error for invalid labels."""
        label_set = LabelSet({"a", "b"})
        label_set.update("c", "d")
        self.assertEqual(label_set, {"a", "b", "c", "d"})

        with self.assertRaises(exceptions.InvalidLabelException):
            label_set.update("e", "f_")

    def test_any_label_starts_with(self):
        """ Ensure starts_with only checks the starts of labels, and doesn't check the starts of sublabels. """
        for label in "a", "b", "d":
            self.assertTrue(self.LABEL_SET.any_label_starts_with(label))

        for label in "c", "e", "f":
            self.assertFalse(self.LABEL_SET.any_label_starts_with(label))

    def test_any_label_ends_swith(self):
        """ Ensure ends_with doesn't check ends of sublabels. """
        for label in "a", "c", "f":
            self.assertTrue(self.LABEL_SET.any_label_ends_with(label))

        for label in "b", "d", "e":
            self.assertFalse(self.LABEL_SET.any_label_ends_with(label))

    def test_serialise(self):
        """Ensure that LabelSets serialise to a list."""
        self.assertEqual(
            json.dumps(self.LABEL_SET, cls=OctueJSONEncoder),
            json.dumps({"_type": "set", "items": ["a", "b-c", "d-e-f"]}),
        )

    def test_serialise_orders_labels(self):
        """Ensure that serialising a LabelSet results in a sorted list."""
        label_set = LabelSet("z hello a c-no")
        self.assertEqual(
            json.dumps(label_set, cls=OctueJSONEncoder),
            json.dumps({"_type": "set", "items": ["a", "c-no", "hello", "z"]}),
        )

    def test_deserialise(self):
        """Test that serialisation is reversible."""
        serialised_label_set = json.dumps(self.LABEL_SET, cls=OctueJSONEncoder)
        deserialised_label_set = LabelSet(json.loads(serialised_label_set, cls=OctueJSONDecoder))
        self.assertEqual(deserialised_label_set, self.LABEL_SET)

    def test_repr(self):
        """Test the representation of a LabelSet appears as expected."""
        self.assertEqual(repr(self.LABEL_SET), f"LabelSet({set(self.LABEL_SET)})")
