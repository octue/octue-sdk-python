from octue.resources.tag import TagSet


class Taggable:
    """ A mixin class allowing objects to be tagged. """

    def __init__(self, *args, tags=None, **kwargs):
        """Constructor for Taggable mixins"""
        super().__init__(*args, **kwargs)
        self._tags = TagSet(tags)

    def add_tags(self, *args):
        """ Adds one or more new tag strings to the object tags. New tags will be cleaned and validated. """
        self._tags.add_tags(*args)

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        """ Overwrite any existing tag set and assign new tags. """
        self._tags = TagSet(tags)
