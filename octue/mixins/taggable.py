from octue.resources.tag import TagDict


class Taggable:
    """A mixin class allowing objects to be tagged."""

    def __init__(self, *args, tags=None, **kwargs):
        self.tags = tags
        super().__init__(*args, **kwargs)

    def add_tags(self, tags=None, **kwargs):
        """Add one or more new tags to the object. New tags will be cleaned and validated."""
        self.tags.update({**(tags or {}), **kwargs})

    @property
    def tags(self):
        """Get the tags of the taggable instance.

        :return iter:
        """
        return self._tags

    @tags.setter
    def tags(self, tags):
        """Overwrite any existing tags and assign the new ones."""
        self._tags = TagDict(tags)
