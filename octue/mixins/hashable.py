import collections.abc
import functools
from blake3 import blake3


_HASH_FUNCTIONS = {
    str: lambda attribute: attribute,
    int: str,
    float: str,
    type(None): lambda attribute: "None",
    dict: lambda attribute: str(sorted(attribute.items())),
}


class Hashable:

    ATTRIBUTES_TO_HASH = None

    @property
    @functools.lru_cache(maxsize=None)
    def blake3_hash(self):
        if not self.ATTRIBUTES_TO_HASH:
            return None

        return self._calculate_blake3_hash()

    def _calculate_blake3_hash(self, blake3_hash=None):
        blake3_hash = blake3_hash or blake3()

        for attribute_name in self.ATTRIBUTES_TO_HASH:

            attribute = getattr(self, attribute_name)

            try:
                items_to_hash = _HASH_FUNCTIONS[type(attribute)](attribute)

            except KeyError:
                if isinstance(attribute, collections.abc.Iterable):
                    items_to_hash = self._hash_iterable(attribute_name, attribute)
                else:
                    raise TypeError(f"Attribute <{attribute_name!r}: {attribute!r}> cannot be hashed.")

            blake3_hash.update(items_to_hash.encode())

        return blake3_hash.hexdigest()

    @staticmethod
    def _hash_iterable(attribute_name, attribute):
        items = tuple(attribute)

        if any(hasattr(item, "blake3_hash") for item in items):

            if not all(hasattr(item, "blake3_hash") for item in items):
                raise ValueError(f"Mixed types in attribute <{attribute_name!r}: {attribute!r}>")

            return str(sorted(subitem.blake3_hash for subitem in items))

        try:
            return str(sorted(items))

        except TypeError:
            raise TypeError(
                f"Attribute needs to be sorted for consistent hash output, but cannot be: "
                f"<{attribute_name!r}: {attribute!r}>"
            )
