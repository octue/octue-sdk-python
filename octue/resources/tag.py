# import json
import re

from octue.exceptions import InvalidTagException
from octue.mixins import Serialisable
from octue.resources.filter_containers import FilterDict


# from collections import UserDict

# from octue.utils.encoders import OctueJSONEncoder


TAG_NAME_PATTERN = re.compile(r"^$|^[A-Za-z0-9][A-Za-z0-9:.\-/]*(?<![./:-])$")

#
# class Tag(Filterable):
#     """A tag starts and ends with a character in [A-Za-z0-9]. It can contain the colon discriminator, forward slashes
#     or hyphens. Empty strings are also valid. More valid examples:
#        system:32
#        angry-marmaduke
#        mega-man:torso:component:12
#     """
#
#     def __init__(self, name):
#         self._name = self._clean(name)
#
#     @property
#     def name(self):
#         return self._name
#
#     def __eq__(self, other):
#         if isinstance(other, str):
#             return self.name == other
#         elif isinstance(other, Tag):
#             return self.name == other.name
#         return False
#
#     def __lt__(self, other):
#         if isinstance(other, str):
#             return self.name < other
#         elif isinstance(other, Tag):
#             return self.name < other.name
#
#     def __gt__(self, other):
#         if isinstance(other, str):
#             return self.name > other
#         elif isinstance(other, Tag):
#             return self.name > other.name
#
#     def __hash__(self):
#         """ Allow Tags to be contained in a set. """
#         return hash(f"{type(self).__name__}{self.name}")
#
#     def __contains__(self, item):
#         return item in self.name
#
#     def __repr__(self):
#         return repr(self.name)
#
#     def starts_with(self, value):
#         """ Does the tag start with the given value? """
#         return self.name.startswith(value)
#
#     def ends_with(self, value):
#         """ Does the tag end with the given value? """
#         return self.name.endswith(value)
#
#     @staticmethod
#     def _clean(name):
#         """ Ensure the tag name is a string and conforms to the tag regex pattern. """
#         if not isinstance(name, str):
#             raise InvalidTagException("Tags must be expressed as a string.")
#
#         cleaned_name = name.strip()
#
#         if not re.match(TAG_NAME_PATTERN, cleaned_name):
#             raise InvalidTagException(
#                 f"Invalid tag '{cleaned_name}'. Tags must contain only characters 'a-z', 'A-Z', '0-9', ':', '.', '/' "
#                 f"and '-'. They must not start with '-', ':', '/' or '.'"
#             )
#
#         return cleaned_name


class TagDict(Serialisable, FilterDict):

    _FILTERABLE_ATTRIBUTE = "data"

    def __setitem__(self, tag, value):
        self._check_tag_format(tag)
        self.data[tag] = value

    def update(self, tags, **kwargs):
        self._check_tag_format(*tags)
        super().update(tags, **kwargs)

    def _check_tag_format(self, *tags):
        for tag in tags:
            if not re.match(TAG_NAME_PATTERN, tag):
                raise InvalidTagException(
                    f"Invalid tag '{tag}'. Tags must contain only characters 'a-z', 'A-Z', '0-9', ':', '.', '/' "
                    f"and '-'. They must not start with '-', ':', '/' or '.'"
                )
