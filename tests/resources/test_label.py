from octue import exceptions
from octue.resources.filter_containers import FilterList, FilterSet
from octue.resources.label import Label, LabelSet
from tests.base import BaseTestCase


class TestLabel(BaseTestCase):
    def test_invalid_labels_cause_error(self):
        """Test that invalid labels cause an error to be raised."""
        for label in ":a", "@", "a_b", "-bah", "humbug:", r"back\slashy", {"not-a": "string"}, "/a", "a/", "blah:3.5.":
            with self.assertRaises(exceptions.InvalidLabelException):
                Label(label)

    def test_valid_labels(self):
        """Test that valid labels instantiate as expected."""
        for label in "hello", "hello:world", "hello-world:goodbye", "HELLO-WORLD", "Asia/Pacific", "blah:3.5":
            Label(label)

    def test_sublabels(self):
        """ Test that sublabels are correctly parsed from labels. """
        self.assertEqual(Label("a:b:c").sublabels, FilterList([Label("a"), Label("b"), Label("c")]))

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
        self.assertTrue(Label("hello").starts_with("h"))
        self.assertFalse(Label("hello").starts_with("e"))

    def test_sublabels_starts_with(self):
        """ Test that the start of sublabels can be checked. """
        self.assertTrue(LabelSet(Label("hello:world").sublabels).any_label_starts_with("w"))
        self.assertFalse(LabelSet(Label("hello:world").sublabels).any_label_starts_with("e"))

    def test_ends_with(self):
        """ Test that the end of a label can be checked. """
        self.assertTrue(Label("hello").ends_with("o"))
        self.assertFalse(Label("hello").ends_with("e"))

    def test_sublabels_ends_with(self):
        """ Test that the end of sublabels can be checked. """
        self.assertTrue(LabelSet(Label("hello:world").sublabels).any_label_ends_with("o"))
        self.assertFalse(LabelSet(Label("hello:world").sublabels).any_label_ends_with("e"))


class TestLabelSet(BaseTestCase):
    LABEL_SET = LabelSet(labels="a b:c d:e:f")

    def test_instantiation_from_space_delimited_string(self):
        """ Test that a LabelSet can be instantiated from a space-delimited string of label names."""
        label_set = LabelSet(labels="a b:c d:e:f")
        self.assertEqual(label_set.labels, FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))

    def test_instantiation_from_iterable_of_strings(self):
        """ Test that a LabelSet can be instantiated from an iterable of strings."""
        label_set = LabelSet(labels=["a", "b:c", "d:e:f"])
        self.assertEqual(label_set.labels, FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))

    def test_instantiation_from_iterable_of_labels(self):
        """ Test that a LabelSet can be instantiated from an iterable of labels."""
        label_set = LabelSet(labels=[Label("a"), Label("b:c"), Label("d:e:f")])
        self.assertEqual(label_set.labels, FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))

    def test_instantiation_from_filter_set_of_strings(self):
        """ Test that a LabelSet can be instantiated from a FilterSet of strings."""
        label_set = LabelSet(labels=FilterSet({"a", "b:c", "d:e:f"}))
        self.assertEqual(label_set.labels, FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))

    def test_instantiation_from_filter_set_of_labels(self):
        """ Test that a LabelSet can be instantiated from a FilterSet of labels."""
        label_set = LabelSet(labels=FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))
        self.assertEqual(label_set.labels, FilterSet({Label("a"), Label("b:c"), Label("d:e:f")}))

    def test_instantiation_from_label_set(self):
        """ Test that a LabelSet can be instantiated from another LabelSet. """
        self.assertEqual(self.LABEL_SET, LabelSet(self.LABEL_SET))

    def test_equality(self):
        """ Ensure two LabelSets with the same labels compare equal. """
        self.assertTrue(self.LABEL_SET == LabelSet(labels="a b:c d:e:f"))

    def test_inequality(self):
        """ Ensure two LabelSets with different labels compare unequal. """
        self.assertTrue(self.LABEL_SET != LabelSet(labels="a"))

    def test_non_label_sets_compare_unequal_to_label_sets(self):
        """ Ensure a LabelSet and a non-LabelSet compare unequal. """
        self.assertFalse(self.LABEL_SET == "a")
        self.assertTrue(self.LABEL_SET != "a")

    def test_iterating_over(self):
        """ Ensure a LabelSet can be iterated over. """
        self.assertEqual(set(self.LABEL_SET), {Label("a"), Label("b:c"), Label("d:e:f")})

    def test_contains_with_string(self):
        """ Ensure we can check that a LabelSet has a certain label using a string form. """
        self.assertTrue("d:e:f" in self.LABEL_SET)
        self.assertFalse("hello" in self.LABEL_SET)

    def test_contains_with_label(self):
        """ Ensure we can check that a LabelSet has a certain label. """
        self.assertTrue(Label("d:e:f") in self.LABEL_SET)
        self.assertFalse(Label("hello") in self.LABEL_SET)

    def test_contains_only_matches_full_labels(self):
        """ Test that the has_label method only matches full labels (i.e. that it doesn't match sublabels or parts of labels."""
        for label in "a", "b:c", "d:e:f":
            self.assertTrue(label in self.LABEL_SET)

        for label in "b", "c", "d", "e", "f":
            self.assertFalse(label in self.LABEL_SET)

    def test_get_sublabels(self):
        """ Test sublabels can be accessed as a new LabelSet. """
        self.assertEqual(LabelSet("meta:sys2:3456 blah").get_sublabels(), LabelSet("meta sys2 3456 blah"))

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

    def test_any_label_contains_searches_for_labels_and_sublabels(self):
        """ Ensure labels and sublabels can be searched for. """
        for label in "a", "b", "d":
            self.assertTrue(self.LABEL_SET.any_label_contains(label))

        for sublabel in "c", "e", "f":
            self.assertTrue(self.LABEL_SET.any_label_contains(sublabel))

    def test_filter(self):
        """ Test that label sets can be filtered. """
        label_set = LabelSet(labels="label1 label2 meta:sys1:1234 meta:sys2:3456 meta:sys2:55")
        self.assertEqual(
            label_set.labels.filter(name__starts_with="meta"),
            FilterSet({Label("meta:sys1:1234"), Label("meta:sys2:3456"), Label("meta:sys2:55")}),
        )

    def test_filter_chaining(self):
        """ Test that filters can be chained. """
        label_set = LabelSet(labels="label1 label2 meta:sys1:1234 meta:sys2:3456 meta:sys2:55")

        filtered_labels_1 = label_set.labels.filter(name__starts_with="meta")
        self.assertEqual(filtered_labels_1, LabelSet("meta:sys1:1234 meta:sys2:3456 meta:sys2:55").labels)

        filtered_labels_2 = filtered_labels_1.filter(name__contains="sys2")
        self.assertEqual(filtered_labels_2, LabelSet("meta:sys2:3456 meta:sys2:55").labels)

        filtered_labels_3 = filtered_labels_1.filter(name__equals="meta:sys2:55")
        self.assertEqual(filtered_labels_3, LabelSet("meta:sys2:55").labels)

    def test_serialise(self):
        """ Ensure that LabelSets are serialised to the string form of a list. """
        self.assertEqual(self.LABEL_SET.serialise(), ["a", "b:c", "d:e:f"])

    def test_serialise_orders_labels(self):
        """Ensure that LabelSets serialise to a list."""
        label_set = LabelSet("z hello a c:no")
        self.assertEqual(label_set.serialise(), ["a", "c:no", "hello", "z"])

    def test_deserialise(self):
        """Test that serialisation is reversible."""
        serialised_label_set = self.LABEL_SET.serialise()
        deserialised_label_set = LabelSet.deserialise(serialised_label_set)
        self.assertEqual(deserialised_label_set, self.LABEL_SET)

    def test_repr(self):
        """Test the representation of a LabelSet appears as expected."""
        self.assertEqual(repr(self.LABEL_SET), f"<LabelSet({repr(self.LABEL_SET.labels)})>")
