import re

from octue.exceptions import InvalidTagException
from octue.mixins import Filterable


# A tag starts and ends with a character in [a-z] or [0-9]. It can contain the colon discriminator or hyphens.
# Empty strings are also valid.
# Valid examples:
#    system:32
#    angry-marmaduke
#    mega-man:torso:component:12
TAG_PATTERN = re.compile(r"^$|^[a-z0-9][a-z0-9:\-]*(?<![:-])$")


class TagGroup(Filterable):
    """ Class to handle a group of tags as a string.
    """

    _ATTRIBUTES_TO_FILTER_BY = ("_tags",)

    def __init__(self, tags, *args, **kwargs):
        """ Construct a TagGroup
        """
        # TODO Call the superclass with *args anad **kwargs, then update everything to using ResourceBase
        if tags is None:
            tags = set()

        elif isinstance(tags, str):
            tags = set(tags.strip().split())

        elif isinstance(tags, TagGroup):
            tags = set(tags._tags)

        # Tags can be some other iterable than a list, but each tag must be a string
        elif not hasattr(tags, "__iter__"):
            raise InvalidTagException(
                "Tags must be expressed as a whitespace-delimited string or an iterable of strings"
            )

        self._tags = self._clean(tags)
        super().__init__(*args, **kwargs)

    def __str__(self):
        """ Serialise tags to a sorted list string. """
        return self.serialise()

    def __eq__(self, other):
        """ Does this TagGroup have the same tags as another TagGroup? """
        return self._tags == other._tags

    def __iter__(self):
        """ Iterate over the tags in the TagGroup. """
        yield from self._tags

    def __len__(self):
        return len(self._tags)

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
        cleaned_tags = set()
        for tag in tags:
            cleaned_tag = tag.strip()
            if not re.match(TAG_PATTERN, cleaned_tag):
                raise InvalidTagException(
                    f"Invalid tag '{cleaned_tag}'. Tags must contain only characters 'a-z', '0-9', ':' and '-'. They must not start with '-' or ':'."
                )
            cleaned_tags.add(cleaned_tag)

        return cleaned_tags

    def _yield_subtags(self, tags=None):
        """ Yield the colon-separated subtags of a tag as strings, including the main tag. """
        for tag in tags or self._tags:
            yield from tag.split(":")

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated.
        """
        self._tags |= self._clean(args)

    def has_tag(self, tag):
        """ Returns true if any of the tags exactly matches value, allowing test like `if 'a' in TagGroup('a b')`
        """
        return tag in self._tags

    def get_subtags(self, tags=None):
        """ Return a new TagGroup instance with all the subtags. """
        return TagGroup(set(self._yield_subtags(tags or self._tags)))

    def starts_with(self, value, consider_separate_subtags=False, tags=None):
        """ Implement a startswith method that returns true if any of the tags starts with value """
        tags = tags or self._tags

        if not consider_separate_subtags:
            return any(tag.startswith(value) for tag in tags)

        return any(subtag.startswith(value) for subtag in self._yield_subtags(tags))

    def ends_with(self, value, consider_separate_subtags=False, tags=None):
        """ Implement an endswith method that returns true if any of the tags endswith value. """
        tags = tags or self._tags

        if not consider_separate_subtags:
            return any(tag.endswith(value) for tag in tags)

        return any(subtag.endswith(value) for subtag in self._yield_subtags(tags))

    def contains(self, value, tags=None):
        """ Implement a contains method that returns true if any of the tags contains value. """
        return any(value in tag for tag in tags or self._tags)

    def serialise(self):
        """ Serialise tags to a sorted list string. """
        return str(sorted(self))


class Taggable:
    """ A mixin class allowing objects to be tagged
    """

    def __init__(self, *args, tags=None, **kwargs):
        """ Constructor for Taggable mixins
        """
        super().__init__(*args, **kwargs)
        self._tags = TagGroup(tags)

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated.
        """
        self._tags.add_tags(*args)

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        """ Overwrite any existing tag group and assign new tags
        """
        self._tags = TagGroup(tags)
