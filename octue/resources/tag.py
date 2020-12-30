import re
from functools import lru_cache

from octue.exceptions import InvalidTagException
from octue.mixins import Filteree
from octue.resources.filter_containers import FilterSet


TAG_PATTERN = re.compile(r"^$|^[a-z0-9][a-z0-9:\-]*(?<![:-])$")


class Tag(Filteree):
    """ A tag starts and ends with a character in [a-z] or [0-9]. It can contain the colon discriminator or hyphens.
    Empty strings are also valid. More valid examples:
       system:32
       angry-marmaduke
       mega-man:torso:component:12
    """

    _FILTERABLE_ATTRIBUTES = ("name",)

    def __init__(self, name):
        self._name = self._clean(name)

    @property
    def name(self):
        return self._name

    @property
    @lru_cache(maxsize=1)
    def subtags(self):
        return FilterSet({Tag(subtag_name) for subtag_name in (self.name.split(":"))})

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == Tag(other).name
        elif isinstance(other, Tag):
            return self.name == other.name
        return False

    def __lt__(self, other):
        if isinstance(other, str):
            return self.name < Tag(other).name
        elif isinstance(other, Tag):
            return self.name < other.name
        return False

    def __gt__(self, other):
        if isinstance(other, str):
            return self.name > Tag(other).name
        elif isinstance(other, Tag):
            return self.name > other.name
        return False

    def __hash__(self):
        return hash(f"{type(self).__name__}{self.name}")

    def __contains__(self, item):
        return item in self.name

    def __repr__(self):
        return repr(self.name)

    def starts_with(self, value, consider_separate_subtags=False):
        """ Implement a startswith method that returns true if any of the tags starts with value """
        if not consider_separate_subtags:
            return self.name.startswith(value)

        return any(subtag.startswith(value) for subtag in self.subtags)

    def ends_with(self, value, consider_separate_subtags=False):
        """ Implement an endswith method that returns true if any of the tags endswith value. """
        if not consider_separate_subtags:
            return self.name.endswith(value)

        return any(subtag.ends_with(value) for subtag in self.subtags)

    def contains(self, value):
        """ Implement a contains method that returns true if any of the tags contains value. """
        return value in self.name

    @staticmethod
    def _clean(name):
        """ Private method to clean up an iterable of tags into a list of cleaned tags
        """
        # Check they're strings
        if not isinstance(name, str):
            raise InvalidTagException("Tags must be expressed as a string")

        # Strip leading and trailing whitespace, ensure they match the regex
        cleaned_name = name.strip()

        if not re.match(TAG_PATTERN, cleaned_name):
            raise InvalidTagException(
                f"Invalid tag '{cleaned_name}'. Tags must contain only characters 'a-z', '0-9', ':' and '-'. They must "
                f"not start with '-' or ':'."
            )

        return cleaned_name


class TagGroup:
    """ Class to handle a group of tags as a string.
    """

    _FILTERSET_ATTRIBUTE = "tags"

    def __init__(self, tags, *args, **kwargs):
        """ Construct a TagGroup
        """
        # TODO Call the superclass with *args anad **kwargs, then update everything to using ResourceBase
        if tags is None:
            self.tags = FilterSet()

        elif isinstance(tags, str):
            self.tags = FilterSet(Tag(tag) for tag in tags.strip().split())

        elif isinstance(tags, TagGroup):
            self.tags = FilterSet(tags.tags)

        # Tags can be some other iterable than a list, but each tag must be a string
        elif hasattr(tags, "__iter__"):
            self.tags = FilterSet(tags)

        else:
            raise InvalidTagException(
                "Tags must be expressed as a whitespace-delimited string or an iterable of strings or Tag instances."
            )

    def __str__(self):
        """ Serialise tags to a sorted list string. """
        return self.serialise()

    def __eq__(self, other):
        """ Does this TagGroup have the same tags as another TagGroup? """
        if not isinstance(other, TagGroup):
            return False
        return self.tags == other.tags

    def __iter__(self):
        """ Iterate over the tags in the TagGroup. """
        yield from self.tags

    def __len__(self):
        return len(self.tags)

    def __repr__(self):
        return f"<TagGroup({self.tags})>"

    def _yield_subtags(self):
        """ Yield the colon-separated subtags of a tag as strings, including the main tag. """
        for tag in self.tags:
            yield from tag.subtags

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated.
        """
        self.tags |= {Tag(arg) for arg in args}

    def has_tag(self, tag):
        """ Returns true if any of the tags exactly matches value, allowing test like `if 'a' in TagGroup('a b')`
        """
        return tag in self.tags or Tag(tag) in self.tags

    def get_subtags(self):
        """ Return a new TagGroup instance with all the subtags. """
        return TagGroup(FilterSet(self._yield_subtags()))

    def starts_with(self, value, consider_separate_subtags=False):
        """ Implement a startswith method that returns true if any of the tags starts with value """
        if not consider_separate_subtags:
            return any(tag.starts_with(value) for tag in self.tags)

        return any(subtag.starts_with(value) for subtag in self._yield_subtags())

    def ends_with(self, value, consider_separate_subtags=False):
        """ Implement an endswith method that returns true if any of the tags endswith value. """
        if not consider_separate_subtags:
            return any(tag.ends_with(value) for tag in self.tags)

        return any(subtag.ends_with(value) for subtag in self._yield_subtags())

    def any_tag_contains(self, value):
        """ Implement a contains method that returns true if any of the tags contains value. """
        return any(value in tag for tag in self.tags)

    def serialise(self):
        """ Serialise tags to a sorted list string. """
        return str(sorted(self))
