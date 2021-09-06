import json
import re
from collections import UserString

from octue.exceptions import InvalidLabelException
from octue.resources.filter_containers import FilterSet


LABEL_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*(?<!-)$")


class Label(UserString):
    """A label starts and ends with a character in [A-Za-z0-9] and can contain hyphens e.g. angry-marmaduke

    :param str name: the text of the label
    :return None:
    """

    def __init__(self, name):
        super().__init__(self._clean(name))

    @staticmethod
    def _clean(name):
        """Clean the label name, making sure it is a string and conforms to the label regex pattern.

        :param str name: name of the label to check and clean
        :return str:
        """
        if not isinstance(name, str):
            raise InvalidLabelException("Labels must be expressed as a string.")

        cleaned_name = name.strip()

        if not re.match(LABEL_PATTERN, cleaned_name):
            raise InvalidLabelException(
                f"Invalid label '{cleaned_name}'. Labels must contain only lowercase characters 'a-z', '0-9', and '-'. "
                f"They must not start with '-'."
            )

        return cleaned_name


class LabelSet(set):
    """Class to handle a set of labels.

    :param iter(str|Label)|LabelSet labels: labels to add to the set
    :return None:
    """

    def __init__(self, labels=None):
        # TODO Call the superclass with *args and **kwargs, then update everything to using ResourceBase
        labels = labels or FilterSet()

        # JSON-encoded list of label names, or space-delimited string of label names.
        if isinstance(labels, str):
            try:
                labels = FilterSet(Label(label) for label in json.loads(labels))
            except json.decoder.JSONDecodeError:
                labels = FilterSet(Label(label) for label in labels.strip().split())

        elif isinstance(labels, LabelSet):
            labels = FilterSet(labels)

        # Labels can be some other iterable than a list, but each label must be a Label or string.
        elif hasattr(labels, "__iter__"):
            labels = FilterSet(label if isinstance(label, Label) else Label(label) for label in labels)

        else:
            raise InvalidLabelException(
                "Labels must be expressed as a whitespace-delimited string or an iterable of strings or Label instances."
            )

        super().__init__(labels)

    def add(self, label):
        """Add a label string to the set.

        :param str label: the label to add
        :return None:
        """
        super().add(Label(label))

    def update(self, *labels):
        """Add one or more new label strings to the object labels. New labels will be cleaned and validated.

        :param str *labels: a variable number of string labels
        :return None:
        """
        super().update({Label(label) for label in labels})

    def any_label_starts_with(self, value):
        """Return `True` if any of the labels starts with the value.

        :param str value: value to check
        :return bool:
        """
        return any(label.startswith(value) for label in self)

    def any_label_ends_with(self, value):
        """Return `True` if any of the labels ends with the value.

        :param str value: value to check
        :return bool:
        """
        return any(label.endswith(value) for label in self)

    def any_label_contains(self, value):
        """Return `True` if any of the labels contains the value.

        :param str value: value to check
        :return bool:
        """
        return any(value in label for label in self)
