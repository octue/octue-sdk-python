from octue.resources.label import LabelSet


class Labelable:
    """ A mixin class allowing objects to be labelled. """

    def __init__(self, *args, labels=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._labels = LabelSet(labels)

    def add_labels(self, *args):
        """ Adds one or more new label strings to the object labels. New labels will be cleaned and validated. """
        self._labels.add_labels(*args)

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, labels):
        """ Overwrite any existing label set and assign new label. """
        self._labels = LabelSet(labels)
