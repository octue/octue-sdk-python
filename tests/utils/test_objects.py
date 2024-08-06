from unittest import TestCase
from unittest.mock import Mock

from octue.utils.objects import dictionary_product, get_nested_attribute, getattr_or_subscribe


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


class TestDictionaryProduct(TestCase):
    def test_with_no_iterables(self):
        """Test providing no iterables."""
        self.assertEqual(list(dictionary_product()), [{}])

    def test_with_empty_iterables(self):
        """Test providing only empty iterables results in an empty output."""
        self.assertEqual(list(dictionary_product(empty=[], iterables=[])), [])

    def test_with_one_iterable(self):
        """Test providing one iterable gives an output of each of the iterable's values individually."""
        self.assertEqual(
            list(dictionary_product(one=["a", "b"])),
            [{"one": "a"}, {"one": "b"}],
        )

    def test_with_two_iterables(self):
        """Test providing two iterables gives the combinations of the iterables' values."""
        self.assertEqual(
            list(dictionary_product(one=["a", "b"], two=["c", "d"])),
            [
                {"one": "a", "two": "c"},
                {"one": "a", "two": "d"},
                {"one": "b", "two": "c"},
                {"one": "b", "two": "d"},
            ],
        )

    def test_none_iterable_ignored_by_default(self):
        """Test that a `None` iterable is ignored by default."""
        self.assertEqual(
            list(dictionary_product(one=["a", "b"], two=["c", "d"], three=None)),
            [
                {"one": "a", "two": "c"},
                {"one": "a", "two": "d"},
                {"one": "b", "two": "c"},
                {"one": "b", "two": "d"},
            ],
        )

    def test_none_iterable_not_ignored_if_keep_none_values(self):
        """Test that a `None` iterable isn't ignored and is included in the outputs without increasing the number of
        outputs if `keep_none_values=True`.
        """
        self.assertEqual(
            list(dictionary_product(one=["a", "b"], two=["c", "d"], three=None, keep_none_values=True)),
            [
                {"one": "a", "two": "c", "three": None},
                {"one": "a", "two": "d", "three": None},
                {"one": "b", "two": "c", "three": None},
                {"one": "b", "two": "d", "three": None},
            ],
        )

    def test_with_only_none_iterables(self):
        """Test that only providing `None` iterables results in an iterable of length 1."""
        self.assertEqual(
            list(dictionary_product(one=None, two=None, three=None, keep_none_values=True)),
            [{"one": None, "two": None, "three": None}],
        )
