import base64
import collections.abc
import datetime

from google_crc32c import Checksum


EMPTY_STRING_HASH_VALUE = "AAAAAA=="

_HASH_PREPARATION_FUNCTIONS = {
    str: lambda attribute: attribute,
    int: str,
    float: str,
    type(None): lambda attribute: "None",
    datetime.datetime: str,
}


class Hashable:

    _ATTRIBUTES_TO_HASH = None
    _HASH_TYPE = "CRC32C"

    def __init__(self, immutable_hash_value=None, *args, **kwargs):
        self._immutable_hash_value = immutable_hash_value
        self._ATTRIBUTES_TO_HASH = self._ATTRIBUTES_TO_HASH or []
        super().__init__(*args, **kwargs)

    @classmethod
    def hash_non_class_object(cls, object_):
        """Use the Hashable class to hash an arbitrary object that isn't an attribute of a class instance.

        :param any object_:
        :return str:
        """

        class Holder(cls):
            _ATTRIBUTES_TO_HASH = ("object_",)

        holder = Holder()
        holder.object_ = object_
        return holder.hash_value

    @property
    def hash_value(self):
        """Get the hash of the instance.

        :return str:
        """
        if self._immutable_hash_value is None:
            return self._calculate_hash()

        return self._immutable_hash_value

    @hash_value.setter
    def hash_value(self, value):
        """Set the hash of the instance to a custom value.

        :param str value:
        :return None:
        """
        if self._immutable_hash_value is not None:
            raise ValueError(f"The hash of {self!r} is immutable - hash_value cannot be set.")

        self._immutable_hash_value = value

    def reset_hash(self):
        """Reset the hash value to the calculated hash (rather than whatever value has been set).

        :return None:
        """
        self._immutable_hash_value = None

    def _calculate_hash(self, hash_=None):
        """Calculate the hash of the sorted attributes in self._ATTRIBUTES_TO_HASH. If `hash_` is not `None` and is
        a hasher object, its in-progress hash will be updated, rather than starting from scratch.

        :param google_crc32c.Checksum hash_:
        :return str:
        """
        hash_ = hash_ or Checksum()

        for attribute_name in sorted(self._ATTRIBUTES_TO_HASH):
            attribute = getattr(self, attribute_name)

            try:
                items_to_hash = _HASH_PREPARATION_FUNCTIONS[type(attribute)](attribute)

            except KeyError:
                if isinstance(attribute, dict):
                    items_to_hash = self._prepare_dictionary_for_hashing(attribute_name, attribute)
                elif isinstance(attribute, collections.abc.Iterable):
                    items_to_hash = self._prepare_iterable_for_hashing(attribute_name, attribute)
                else:
                    raise TypeError(f"Attribute <{attribute_name!r}: {attribute!r}> cannot be hashed.")

            hash_.update(items_to_hash.encode())

        return base64.b64encode(hash_.digest()).decode("utf-8")

    def _prepare_iterable_for_hashing(self, attribute_name, attribute):
        """Prepare an iterable attribute for hashing, using the items' own `hash_value` attributes if available.

        :param str attribute_name:
        :param iter attribute:
        :return str:
        """
        items = tuple(attribute)

        if any(hasattr(item, "hash_value") for item in items):
            if not all(hasattr(item, "hash_value") for item in items):
                raise ValueError(f"Mixed types in attribute <{attribute_name!r}: {attribute!r}>")

            return self._sort_items_and_convert_to_string((item.hash_value for item in items), attribute_name)

        return self._sort_items_and_convert_to_string(items, attribute_name)

    def _prepare_dictionary_for_hashing(self, attribute_name, attribute):
        """Prepare a dictionary attribute for hashing, using its values' own `hash_value` attributes if available.

        :param str attribute_name:
        :param dict attribute:
        :return str:
        """
        items = tuple(attribute.items())

        if any(hasattr(item[1], "hash_value") for item in items):
            if not all(hasattr(item[1], "hash_value") for item in items):
                raise ValueError(f"Mixed types in attribute <{attribute_name!r}: {attribute!r}>")

            return self._sort_items_and_convert_to_string(
                ((key, value.hash_value) for key, value in items),
                attribute_name,
            )

        return self._sort_items_and_convert_to_string(items, attribute_name)

    @staticmethod
    def _sort_items_and_convert_to_string(items, attribute_name):
        """Sort the given items and cast them to a string.

        :param iter items:
        :param str attribute_name:
        :return str:
        """
        try:
            return str(sorted(items))

        except TypeError:
            raise TypeError(
                f"Attribute <{attribute_name!r}: {items!r}> needs to be sorted for consistent hash output, but this "
                f"failed."
            )
