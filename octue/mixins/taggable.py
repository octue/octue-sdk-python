import re

from octue.exceptions import InvalidTagException


# A tag starts and ends with a character in [a-z] or [0-9]. It can contain the colon discriminator or hyphens.
# Empty strings are also valid.
# Valid examples:
#    system:32
#    angry-marmaduke
#    mega-man:torso:component:12
TAG_PATTERN = re.compile(r"^$|^[a-z0-9][a-z0-9:\-]*(?<![:-])$")


class TagGroup:
    """ Class to handle a group of tags as a string.
    """

    def __init__(self, tags):
        """ Construct a TagGroup
        """
        if tags is None:
            tags = []

        elif isinstance(tags, str):
            tags = tags.strip().split()

        elif isinstance(tags, TagGroup):
            # Use the serialise method as a deepcopy
            tags = tags.serialise().split()

        # Tags can be some other iterable than a list, but each tag must be a string
        elif not hasattr(tags, "__iter__"):
            raise InvalidTagException(
                "Tags must be expressed as a whitespace-delimited string or an iterable of strings"
            )

        self._tags = self._clean(tags)

    @staticmethod
    def _clean(tags):
        """ Private method to clean up an iterable of tags into a list of cleaned tags
        """
        # Check they're strings
        if not all(isinstance(tag, str) for tag in tags):
            raise InvalidTagException(
                "Tags must be expressed as a whitespace-delimited string or an iterable of strings"
            )

        # Strip leading and trailing whitespace, ensure they match the regex
        cleaned_tags = []
        for tag in tags:
            cleaned_tag = tag.strip()
            if not re.match(TAG_PATTERN, cleaned_tag):
                raise InvalidTagException(
                    "Tags must contain only characters 'a-z', '0-9', ':' and '-'. They must not start with '-' or ':'."
                )
            cleaned_tags.append(cleaned_tag)

        return cleaned_tags

    def __str__(self):
        """ Represents tags as a space delimited string
        """
        return self.serialise()

    def serialise(self):
        """ Serialises tags as a space delimited string, NOT as a list. Strips end whitespace.
        """
        return " ".join(self._tags).strip()

    def startswith(self, value):
        """ Implement a startswith method that returns true if any of the tags starts with value
        """
        return any(tag.startswith(value) for tag in self._tags)

    def endswith(self, value):
        """ Implement an endswith method that returns true if any of the tags endswith value
        """
        return any(tag.endswith(value) for tag in self._tags)

    def contains(self, value):
        """ Implement a contains method that returns true if any of the tags contains value
        """
        return any(value in tag for tag in self._tags)

    def __contains__(self, value):
        """ Returns true if any of the tags exactly matches value, allowing test like `if 'a' in TagGroup('a b')`
        """
        return any(value == tag for tag in self._tags)

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated.
        """
        self._tags += self._clean(args)


class Taggable:
    """ A mixin class allowing objects to be tagged
    """

    def __init__(self, tags=None, **kwargs):
        """ Constructor for Taggable mixins
        """
        super().__init__(**kwargs)
        self._tags = TagGroup(tags)

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated.
        """
        self._tags.add_tags(*args)

    @property
    def tags(self):
        """ Space delimited tag string
        """
        return self._tags

    @tags.setter
    def tags(self, tags):
        """ Overwrite any existing tag group and assign new tags
        """
        self._tags = TagGroup(tags)
