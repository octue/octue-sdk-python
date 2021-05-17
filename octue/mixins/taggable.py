from octue.resources.tag import TagDict


class Taggable:
    """A mixin class allowing objects to be tagged."""

    def __init__(self, tags=None):
        self.tags = tags

    def add_tags(self, tags=None, **kwargs):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated. """
        self.tags.update({**(tags or {}), **kwargs})

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        """ Overwrite any existing tag set and assign new tag. """
        self._tags = TagDict(tags)
