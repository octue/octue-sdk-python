import json
import re
from collections import UserDict

from octue.exceptions import InvalidTagException
from octue.mixins import Serialisable
from octue.utils.encoders import OctueJSONEncoder


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
                    f"Invalid tag '{tag}'. Tags must contain only characters 'a-z', 'A-Z', '0-9', and '_'. They must "
                    f"not start with '_'."
                )

    def serialise(self, to_string=False, **kwargs):
        """Serialise a TagDict to a JSON dictionary or string.

        :param bool to_string:
        :return str|dict:
        """
        string = json.dumps(self.data, cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)

        if to_string:
            return string

        return json.loads(string)
