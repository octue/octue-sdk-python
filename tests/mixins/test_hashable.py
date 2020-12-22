from octue.mixins import Hashable
from ..base import BaseTestCase


class EmptyClass:
    pass


class TypeWithBLAKE3Hash:
    blake3_hash = None


class TypeWithIterable(Hashable):
    ATTRIBUTES_TO_HASH = ("my_iterable",)


class HashableTestCase(BaseTestCase):
    def test_no_attributes_to_hash_results_in_no_hash(self):
        """ Assert classes with Hashable mixed in but no attributes to hash give None for their hash. """
        self.assertIsNone(TypeWithBLAKE3Hash().blake3_hash)

    def test_non_hashable_type_results_in_type_error(self):
        class MyClass(Hashable):
            ATTRIBUTES_TO_HASH = ("a_class",)

        my_class = MyClass()
        my_class.a_class = EmptyClass()

        with self.assertRaises(TypeError):
            my_class.blake3_hash

    def test_iterable_attribute_with_mixed_types_raises_value_error(self):

        my_class = TypeWithIterable()
        my_class.my_iterable = [1, 2, TypeWithBLAKE3Hash()]

        with self.assertRaises(ValueError):
            my_class.blake3_hash

    def test_unsortable_iterable_attribute_raises_type_error(self):
        my_class = TypeWithIterable()
        my_class.my_iterable = [EmptyClass(), EmptyClass()]

        with self.assertRaises(TypeError):
            my_class.blake3_hash
