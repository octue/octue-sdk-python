from octue.mixins import Hashable
from ..base import BaseTestCase


class EmptyClass:
    pass


class TypeWithHash:
    hash_value = None


class TypeWithIterable(Hashable):
    _ATTRIBUTES_TO_HASH = ("my_iterable",)


class HashableTestCase(BaseTestCase):
    def test_no_attributes_to_hash_results_in_no_hash(self):
        """ Assert classes with Hashable mixed in but no attributes to hash give None for their hash. """
        self.assertIsNone(TypeWithHash().hash_value)

    def test_non_hashable_type_results_in_type_error(self):
        """ Ensure trying to hash unhashable attributes results in a TypeError. """

        class MyClass(Hashable):
            _ATTRIBUTES_TO_HASH = ("a_class",)

        my_class = MyClass()
        my_class.a_class = EmptyClass()

        with self.assertRaises(TypeError):
            my_class.hash_value

    def test_iterable_attribute_with_mixed_types_raises_value_error(self):
        """ Ensure trying to hash iterable attributes containing a mix between types with a 'hash_value' attribute and
        types that don't results in a TypeError. """
        my_class = TypeWithIterable()
        my_class.my_iterable = [1, 2, TypeWithHash()]

        with self.assertRaises(ValueError):
            my_class.hash_value

    def test_unsortable_iterable_attribute_raises_type_error(self):
        """ Ensure trying to hash unsortable iterable attributes results in a TypeError. """
        my_class = TypeWithIterable()
        my_class.my_iterable = [EmptyClass(), EmptyClass()]

        with self.assertRaises(TypeError):
            my_class.hash_value
