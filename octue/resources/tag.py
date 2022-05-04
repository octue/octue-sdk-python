import re
from collections import UserDict

from octue.exceptions import InvalidTagException
from octue.mixins import Serialisable


TAG_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]*(?<!_)$")


class TagDict(Serialisable, UserDict):
    def __setitem__(self, name, value):
        """Add a tag to the TagDict via subscription.

        :param str name: name of tag
        :param any value: value of tag
        :return None:
        """
        self._check_tag_format(name)
        super().__setitem__(name, value)

    def update(self, tags, **kwargs):
        """Add multiple tags to the TagDict from another dictionary or as keyword arguments.

        :param dict|TagDict tags: tags to add
        :param **kwargs: {str: any} pairs of tags as keyword arguments e.g. `my_tag=7`
        :return None:
        """
        self._check_tag_format(*tags)
        super().update(tags, **kwargs)

    def _check_tag_format(self, *tags):
        """Check if each tag conforms to the tag name pattern.

        :param *tags: any number of str items to check
        :return:
        """
        for tag in tags:
            if not re.match(TAG_NAME_PATTERN, tag):
                raise InvalidTagException(
                    f"Invalid tag '{tag}'. Tags must contain only lowercase characters 'a-z', '0-9', and '_'. They "
                    f"must not start with '_'."
                )

    def to_primitive(self, **kwargs):
        """Convert a TagDict to a python primitive.

        :return str:
        """
        return self.data
