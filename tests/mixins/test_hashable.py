from octue.mixins import Hashable
from octue.mixins.hashable import EMPTY_STRING_HASH_VALUE
from ..base import BaseTestCase


class EmptyClass:
    pass


class TypeWithNoAttributesToHash(Hashable):
    pass


class TypeWithAttributeToHash(Hashable):
    _ATTRIBUTES_TO_HASH = ("a",)
    a = 7


class TypeWithIterable(Hashable):
    _ATTRIBUTES_TO_HASH = ("my_iterable",)


class HashableTestCase(BaseTestCase):
    def test_no_attributes_to_hash_results_in_trivial_hash_value(self):
        """Assert classes with Hashable mixed in but no attributes to hash have a trivial hash value."""
        self.assertEqual(TypeWithNoAttributesToHash().hash_value, EMPTY_STRING_HASH_VALUE)

    def test_non_hashable_type_results_in_type_error(self):
        """ Ensure trying to hash unhashable attributes results in a TypeError. """

        class MyClass(Hashable):
            _ATTRIBUTES_TO_HASH = ("a_class",)

        my_class = MyClass()
        my_class.a_class = EmptyClass()

        with self.assertRaises(TypeError):
            my_class.hash_value

    def test_iterable_attribute_with_mixed_types_raises_value_error(self):
        """Ensure trying to hash iterable attributes containing a mix between types with a 'hash_value' attribute and
        types that don't results in a TypeError."""
        my_class = TypeWithIterable()
        my_class.my_iterable = [1, 2, TypeWithNoAttributesToHash()]

        with self.assertRaises(ValueError):
            my_class.hash_value

    def test_unsortable_iterable_attribute_raises_type_error(self):
        """ Ensure trying to hash unsortable iterable attributes results in a TypeError. """
        my_class = TypeWithIterable()
        my_class.my_iterable = [EmptyClass(), EmptyClass()]

        with self.assertRaises(TypeError):
            my_class.hash_value

    def test_set_hash_value(self):
        """Test that hash values can be set."""
        type_with_no_attributes_to_hash = TypeWithNoAttributesToHash()
        type_with_no_attributes_to_hash.hash_value = "hello"
        self.assertEqual(type_with_no_attributes_to_hash.hash_value, "hello")

    def test_set_hash_value_overrides_calculated_hash(self):
        """Test that hash values that are set override the calculated value."""
        type_with_hash = TypeWithAttributeToHash()
        self.assertEqual(len(type_with_hash.hash_value), 8)

        type_with_hash.hash_value = "hello"
        self.assertEqual(type_with_hash.hash_value, "hello")

    def test_reset_hash(self):
        """Test that hash values can be set and then reset to the calculated value."""
        type_with_hash = TypeWithAttributeToHash()
        original_calculated_hash = type_with_hash.hash_value
        self.assertEqual(len(original_calculated_hash), 8)

        type_with_hash.hash_value = "hello"
        self.assertEqual(type_with_hash.hash_value, "hello")

        type_with_hash.reset_hash()
        self.assertEqual(type_with_hash.hash_value, original_calculated_hash)

    def test_hash_value_cannot_be_set_if_hashable_has_immutable_hash_value(self):
        """Test that the hash value of a hashable instance with an immutable hash value cannot be set."""
        hashable = Hashable(immutable_hash_value="blue")

        with self.assertRaises(ValueError):
            hashable.hash_value = "red"
