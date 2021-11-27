from octue.resources.label import LabelSet


class Labelable:
    """A mixin class allowing objects to be labelled."""

    def __init__(self, *args, labels=None, **kwargs):
        self.labels = labels
        super().__init__(*args, **kwargs)

    def add_labels(self, *args):
        """Add one or more new labels to the object. New labels will be cleaned and validated."""
        self.labels.update(*args)

    @property
    def labels(self):
        """Get the labels of the labelled object.

        :return iter:
        """
        return self._labels

    @labels.setter
    def labels(self, labels):
        """Overwrite any existing label set and assign new labels."""
        self._labels = LabelSet(labels)
