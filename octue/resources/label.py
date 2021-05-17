import json
import re
from functools import lru_cache

from octue.exceptions import InvalidLabelException
from octue.mixins import Filterable
from octue.resources.filter_containers import FilterList, FilterSet
from octue.utils.encoders import OctueJSONEncoder


LABEL_PATTERN = re.compile(r"^$|^[A-Za-z0-9][A-Za-z0-9:.\-/]*(?<![./:-])$")


class Label(Filterable):
    """A label starts and ends with a character in [A-Za-z0-9]. It can contain the colon discriminator, forward slashes
    or hyphens. Empty strings are also valid. More valid examples:
       system:32
       angry-marmaduke
       mega-man:torso:component:12
    """

    def __init__(self, name):
        self._name = self._clean(name)

    @property
    def name(self):
        return self._name

    @property
    @lru_cache(maxsize=1)
    def sublabels(self):
        """Return the sublabels of the label in order as a FilterList (e.g. FilterList(['a', 'b', 'c']) for Label('a:b:c').

        :return FilterList(Label):
        """
        return FilterList(Label(sublabel_name) for sublabel_name in self.name.split(":"))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, Label):
            return self.name == other.name
        return False

    def __lt__(self, other):
        if isinstance(other, str):
            return self.name < other
        elif isinstance(other, Label):
            return self.name < other.name

    def __gt__(self, other):
        if isinstance(other, str):
            return self.name > other
        elif isinstance(other, Label):
            return self.name > other.name

    def __hash__(self):
        """ Allow Labels to be contained in a set. """
        return hash(f"{type(self).__name__}{self.name}")

    def __contains__(self, item):
        return item in self.name

    def __repr__(self):
        return repr(self.name)

    def starts_with(self, value):
        """ Does the label start with the given value? """
        return self.name.startswith(value)

    def ends_with(self, value):
        """ Does the label end with the given value? """
        return self.name.endswith(value)

    @staticmethod
    def _clean(name):
        """ Ensure the label name is a string and conforms to the label regex pattern. """
        if not isinstance(name, str):
            raise InvalidLabelException("Labels must be expressed as a string.")

        cleaned_name = name.strip()

        if not re.match(LABEL_PATTERN, cleaned_name):
            raise InvalidLabelException(
                f"Invalid label '{cleaned_name}'. Labels must contain only characters 'a-z', 'A-Z', '0-9', ':', '.', '/' "
                f"and '-'. They must not start with '-', ':', '/' or '.'"
            )

        return cleaned_name


class LabelSet:
    """ Class to handle a set of labels as a string. """

    _FILTERSET_ATTRIBUTE = "labels"

    def __init__(self, labels=None):
        """ Construct a LabelSet. """
        # TODO Call the superclass with *args and **kwargs, then update everything to using ResourceBase
        labels = labels or FilterSet()

        # JSON-encoded list of label names, or space-delimited string of label names.
        if isinstance(labels, str):
            try:
                self.labels = FilterSet(Label(label) for label in json.loads(labels))
            except json.decoder.JSONDecodeError:
                self.labels = FilterSet(Label(label) for label in labels.strip().split())

        elif isinstance(labels, LabelSet):
            self.labels = FilterSet(labels.labels)

        # Labels can be some other iterable than a list, but each label must be a Label or string.
        elif hasattr(labels, "__iter__"):
            self.labels = FilterSet(label if isinstance(label, Label) else Label(label) for label in labels)

        else:
            raise InvalidLabelException(
                "Labels must be expressed as a whitespace-delimited string or an iterable of strings or Label instances."
            )

    def __eq__(self, other):
        """ Does this LabelSet have the same labels as another LabelSet? """
        if not isinstance(other, LabelSet):
            return False
        return self.labels == other.labels

    def __iter__(self):
        """ Iterate over the labels in the LabelSet. """
        yield from self.labels

    def __len__(self):
        return len(self.labels)

    def __contains__(self, label):
        """ Return True if any of the labels exactly matches value, allowing test like `if 'a' in LabelSet('a b')`. """
        if isinstance(label, str):
            return Label(label) in self.labels
        if isinstance(label, Label):
            return label in self.labels

    def __repr__(self):
        return f"<{type(self).__name__}({self.labels})>"

    def add_labels(self, *args):
        """Adds one or more new label strings to the object labels. New labels will be cleaned and validated."""
        self.labels |= {Label(arg) for arg in args}

    def get_sublabels(self):
        """ Return a new LabelSet instance with all the sublabels. """
        return LabelSet(sublabel for label in self for sublabel in label.sublabels)

    def any_label_starts_with(self, value):
        """ Implement a startswith method that returns true if any of the labels starts with value """
        return any(label.starts_with(value) for label in self)

    def any_label_ends_with(self, value):
        """ Implement an endswith method that returns true if any of the labels endswith value. """
        return any(label.ends_with(value) for label in self)

    def any_label_contains(self, value):
        """ Return True if any of the labels contains value. """
        return any(value in label for label in self)

    def filter(self, filter_name=None, filter_value=None):
        """Filter the labels with the given filter for the given value.

        :param str filter_name:
        :param any filter_value:
        :return octue.resources.filter_containers.FilterSet:
        """
        return self.labels.filter(filter_name=filter_name, filter_value=filter_value)

    def serialise(self, to_string=False, **kwargs):
        """Serialise to a sorted list of label names.

        :param bool to_string:
        :return list|str:
        """
        string = json.dumps(
            sorted(label.name for label in self.labels), cls=OctueJSONEncoder, sort_keys=True, indent=4, **kwargs
        )

        if to_string:
            return string

        return json.loads(string)

    @classmethod
    def deserialise(cls, serialised_labelset):
        """Deserialise from a sorted list of label names.

        :param list serialised_labelset:
        :return LabelSet:
        """
        return cls(labels=serialised_labelset)
