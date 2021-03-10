import collections.abc
from blake3 import blake3


_HASH_PREPARATION_FUNCTIONS = {
    str: lambda attribute: attribute,
    int: str,
    float: str,
    type(None): lambda attribute: "None",
    dict: lambda attribute: str(sorted(attribute.items())),
}


class Hashable:

    _ATTRIBUTES_TO_HASH = None
    _HASH_TYPE = "BLAKE3"

    def __init__(self, hash_value=None, *args, **kwargs):
        self._hash_value = hash_value
        super().__init__(*args, **kwargs)

    @classmethod
    def hash_non_class_object(cls, object_):
        """ Use the Hashable class to hash an arbitrary object that isn't an attribute of a class instance. """

        class Holder(cls):
            _ATTRIBUTES_TO_HASH = ("object_",)

        holder = Holder()
        holder.object_ = object_
        return holder.hash_value

    @property
    def hash_value(self):
        """Get the hash of the instance."""
        if self._hash_value:
            return self._hash_value

        if not self._ATTRIBUTES_TO_HASH:
            return None

        self._hash_value = self._calculate_hash()
        return self._hash_value

    @hash_value.setter
    def hash_value(self, value):
        """Set the hash of the instance to a custom value.

        :param str value:
        :return None:
        """
        self._hash_value = value

    def reset_hash(self):
        """Reset the hash value to the calculated hash (rather than whatever value has been set).

        :return None:
        """
        self._hash_value = self._calculate_hash()

    def _calculate_hash(self, hash_=None):
        """Calculate the BLAKE3 hash of the sorted attributes in self._ATTRIBUTES_TO_HASH. If hash_ is not None and is
        a BLAKE3 hasher object, its in-progress hash will be updated, rather than starting from scratch."""
        hash_ = hash_ or blake3()

        for attribute_name in sorted(self._ATTRIBUTES_TO_HASH):
            attribute = getattr(self, attribute_name)

            try:
                items_to_hash = _HASH_PREPARATION_FUNCTIONS[type(attribute)](attribute)

            except KeyError:
                if isinstance(attribute, collections.abc.Iterable):
                    items_to_hash = self._prepare_iterable_for_hashing(attribute_name, attribute)
                else:
                    raise TypeError(f"Attribute <{attribute_name!r}: {attribute!r}> cannot be hashed.")

            hash_.update(items_to_hash.encode())

        return hash_.hexdigest()

    @staticmethod
    def _prepare_iterable_for_hashing(attribute_name, attribute):
        """ Prepare an iterable attribute for hashing, using the items' own BLAKE3 hashes if available. """
        items = tuple(attribute)

        if any(hasattr(item, "hash_value") for item in items):

            if not all(hasattr(item, "hash_value") for item in items):
                raise ValueError(f"Mixed types in attribute <{attribute_name!r}: {attribute!r}>")

            return str(sorted(item.hash_value for item in items))

        try:
            return str(sorted(items))

        except TypeError:
            raise TypeError(
                f"Attribute <{attribute_name!r}: {attribute!r}> needs to be sorted for consistent hash output, but this"
                f"failed."
            )
