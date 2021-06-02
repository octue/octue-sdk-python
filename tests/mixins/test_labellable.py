from octue import exceptions
from octue.mixins import Labelable
from octue.resources.label import Label, LabelSet
from ..base import BaseTestCase


class MyLabelable(Labelable):
    pass


class LabelableTestCase(BaseTestCase):
    def test_instantiates(self):
        """Ensures the class instantiates without arguments"""
        Labelable()

    def test_instantiates_with_labels(self):
        """Ensures datafile inherits correctly from the Labelable class and passes arguments through"""
        labelable = MyLabelable(labels="")
        self.assertEqual(len(labelable.labels), 0)

        labelable = MyLabelable(labels=None)
        self.assertEqual(len(labelable.labels), 0)

        labelable = MyLabelable(labels="a b c")
        self.assertEqual(labelable.labels, {Label("a"), Label("b"), Label("c")})

    def test_instantiates_with_label_set(self):
        """Ensures datafile inherits correctly from the Labelable class and passes arguments through"""
        labelable_1 = MyLabelable(labels="")
        self.assertIsInstance(labelable_1.labels, LabelSet)
        labelable_2 = MyLabelable(labels=labelable_1.labels)
        self.assertFalse(labelable_1 is labelable_2)

    def test_fails_to_instantiates_with_non_iterable(self):
        """Ensures datafile inherits correctly from the Labelable class and passes arguments through"""

        class NoIter:
            pass

        with self.assertRaises(exceptions.InvalidLabelException) as error:
            MyLabelable(labels=NoIter())

        self.assertIn(
            "Labels must be expressed as a whitespace-delimited string or an iterable of strings",
            error.exception.args[0],
        )

    def test_reset_labels(self):
        """Ensures datafile inherits correctly from the Labelable class and passes arguments through"""
        labelable = MyLabelable(labels="a b")
        labelable.labels = "b c"
        self.assertEqual(labelable.labels, {Label("b"), Label("c")})

    def test_valid_labels(self):
        """Ensures valid labels do not raise an error"""
        labelable = MyLabelable()
        labelable.add_labels("a-valid-label")
        labelable.add_labels("label")
        labelable.add_labels("a1829tag")
        labelable.add_labels("1829")
        self.assertEqual(
            labelable.labels,
            {
                Label("a-valid-label"),
                Label("label"),
                Label("a1829tag"),
                Label("1829"),
            },
        )

    def test_mixture_valid_invalid(self):
        """Ensures that adding a variety of labels, some of which are invalid, doesn't partially add them to the object"""
        labelable = MyLabelable()
        labelable.add_labels("first-valid-should-be-added")
        try:
            labelable.add_labels("second-valid-should-not-be-added-because", "-the-third-is-invalid:")

        except exceptions.InvalidLabelException:
            pass

        self.assertEqual({Label("first-valid-should-be-added")}, labelable.labels)
