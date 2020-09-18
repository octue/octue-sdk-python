import re

from octue.exceptions import InvalidTagException


# A tag starts and ends with a character in [a-z] or [0-9]. It can contain the colon discriminator or hyphens.
# Empty strings are also valid.
# Valid examples:
#    system:32
#    angry-marmaduke
#    mega-man:torso:component:12
TAG_PATTERN = re.compile(r"^$|^[a-z0-9][a-z0-9:\-]*(?<![:-])$")


class Taggable:
    """ A mixin class allowing objects to be tagged
    """

    def __init__(self, tags=None, **kwargs):
        self.tags = ""
        if tags is not None:
            self.add_tags(*tags.split(" "))
        super().__init__(**kwargs)

    def add_tags(self, *args):
        """ Adds a new tag string to the object tags. New tag will be validated and whitespacing handled correctly
        """
        tags = ""
        for arg in args:
            if not isinstance(arg, str):
                raise InvalidTagException("Tags must be a string")

            cleaned_arg = arg.strip()
            if not re.match(TAG_PATTERN, cleaned_arg):
                raise InvalidTagException(
                    "Tags must contain only characters 'a-z', '0-9', ':' and '-'. They must not start with '-' or ':'."
                )

            tags = f"{tags} {cleaned_arg}".strip()

        self.tags = f"{self.tags} {tags}".strip()
