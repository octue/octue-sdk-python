import collections.abc
import functools
from blake3 import blake3


_HASH_PREPARATION_FUNCTIONS = {
    str: lambda attribute: attribute,
    int: str,
    float: str,
    type(None): lambda attribute: "None",
    dict: lambda attribute: str(sorted(attribute.items())),
}


class Hashable:

    ATTRIBUTES_TO_HASH = None

    @classmethod
    def hash_non_class_object(cls, object_):
        """ Use the Hashable class to hash an arbitrary object that isn't an attribute of a class instance. """

        class Holder(cls):
            ATTRIBUTES_TO_HASH = ("object",)

        holder = Holder()
        holder.object = object_
        return holder.blake3_hash

    @property
    @functools.lru_cache(maxsize=1)
    def blake3_hash(self):
        """ Get the BLAKE3 hash of the instance. """
        if not self.ATTRIBUTES_TO_HASH:
            return None

        return self._calculate_blake3_hash()

    def _calculate_blake3_hash(self, blake3_hash=None):
        """ Calculate the BLAKE3 hash of the sorted attributes in self.ATTRIBUTES_TO_HASH. """
        blake3_hash = blake3_hash or blake3()

        for attribute_name in sorted(self.ATTRIBUTES_TO_HASH):
            attribute = getattr(self, attribute_name)

            try:
                items_to_hash = _HASH_PREPARATION_FUNCTIONS[type(attribute)](attribute)

            except KeyError:
                if isinstance(attribute, collections.abc.Iterable):
                    items_to_hash = self._prepare_iterable_for_hashing(attribute_name, attribute)
                else:
                    raise TypeError(f"Attribute <{attribute_name!r}: {attribute!r}> cannot be hashed.")

            blake3_hash.update(items_to_hash.encode())

        return blake3_hash.hexdigest()

    @staticmethod
    def _prepare_iterable_for_hashing(attribute_name, attribute):
        """ Prepare an iterable attribute for hashing, using the items` own BLAKE3 hashes if available. """
        items = tuple(attribute)

        if any(hasattr(item, "blake3_hash") for item in items):

            if not all(hasattr(item, "blake3_hash") for item in items):
                raise ValueError(f"Mixed types in attribute <{attribute_name!r}: {attribute!r}>")

            return str(sorted(item.blake3_hash for item in items))

        try:
            return str(sorted(items))

        except TypeError:
            raise TypeError(
                f"Attribute <{attribute_name!r}: {attribute!r}> needs to be sorted for consistent hash output, but this"
                f"failed."
            )
