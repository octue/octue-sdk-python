import json
import re

from octue.exceptions import InvalidTagException
from octue.mixins import Serialisable
from octue.resources.filter_containers import FilterDict
from octue.utils.encoders import OctueJSONEncoder


TAG_NAME_PATTERN = re.compile(r"^$|^[A-Za-z0-9][A-Za-z0-9:.\-/]*(?<![./:-])$")


class TagDict(Serialisable, FilterDict):
    def __setitem__(self, tag, value):
        self._check_tag_format(tag)
        super().__setitem__(tag, value)

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

    def serialise(self, to_string=False, **kwargs):
        """Serialise a TagDict to a JSON dictionary or a string version of one.

        :param bool to_string:
        :return str|dict:
        """
        string = json.dumps(self.data, cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs)

        if to_string:
            return string

        return json.loads(string)
