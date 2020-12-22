import collections
import functools
from blake3 import blake3


class Hashable:

    ATTRIBUTES_TO_HASH = None

    @property
    @functools.lru_cache(maxsize=None)
    def blake3_hash(self):
        return self._calculate_blake3_hash()

    def _calculate_blake3_hash(self):
        blake3_hash = blake3()

        for attribute_name in self.ATTRIBUTES_TO_HASH:

            attribute = getattr(self, attribute_name)

            if isinstance(attribute, dict):
                items_to_hash = str(sorted(attribute.items()))

            elif isinstance(attribute, collections.Iterable):

                items = tuple(attribute)

                if any(hasattr(item, "blake3_hash") for item in items):
                    if all(hasattr(item, "blake3_hash") for item in items):
                        items_to_hash = str(sorted(subitem.blake3_hash for subitem in items))
                    else:
                        raise ValueError(f"Mixed types in attribute: {attribute!r}")

                else:
                    try:
                        items_to_hash = str(sorted(items))
                    except TypeError:
                        raise TypeError(
                            f"Attribute needs to be sorted for consistent hash output, but cannot be: {attribute!r}"
                        )

            else:
                raise TypeError(f"Attribute {attribute!r} cannot be hashed.")

            blake3_hash.update(items_to_hash.encode())

        return blake3_hash.hexdigest()
