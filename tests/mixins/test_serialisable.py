import json
import logging
import os
from tempfile import TemporaryDirectory

from octue.mixins import Serialisable
from ..base import BaseTestCase


class Inherit(Serialisable):
    def __init__(self):
        super().__init__()
        self.id = "id"
        self.logger = logging.getLogger("test_returns_primitive_without_logger_or_protected_fields")
        self.field_to_serialise = 0
        self._field_not_to_serialise = 1


class InheritWithFieldsToSerialise(Inherit):
    _SERIALISE_FIELDS = ("field_to_serialise",)


class SerialisableTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments"""
        Serialisable()

    def test_logger_attribute_excluded_even_when_not_explicitly_excluded(self):
        """Test that a Serialisable with "logger" not explicitly included in the `_EXCLUDE_SERIALISE_FIELDS` class
        variable still excludes the `logger` attribute.
        """

        class SerialisableWithLoggerNotExplicitlyExcluded(Serialisable):
            _EXCLUDE_SERIALISE_FIELDS = []

        self.assertTrue("logger" in SerialisableWithLoggerNotExplicitlyExcluded()._EXCLUDE_SERIALISE_FIELDS)

    def test_returns_primitive_without_logger_or_protected_fields(self):
        """Ensures class instantiates with a UUID()"""
        resource = Inherit()
        serialised = resource.serialise()
        self.assertTrue("id" in serialised.keys())
        self.assertTrue("field_to_serialise" in serialised.keys())
        self.assertFalse("logger" in serialised.keys())
        self.assertFalse("_field_not_to_serialise" in serialised.keys())

    def test_serialise_only_attrs(self):
        """Restricts the id field, which would normally be serialised"""
        resource = InheritWithFieldsToSerialise()
        serialised = resource.serialise()
        self.assertFalse("id" in serialised.keys())
        self.assertTrue("field_to_serialise" in serialised.keys())
        self.assertFalse("logger" in serialised.keys())
        self.assertFalse("_field_not_to_serialise" in serialised.keys())

    def test_serialise_to_string(self):
        """Restricts the id field, which would normally be serialised"""
        resource = InheritWithFieldsToSerialise()
        serialised = resource.serialise(to_string=True)
        self.assertIsInstance(serialised, str)

    def test_serialise_to_file(self):
        """Restricts the id field, which would normally be serialised"""

        with TemporaryDirectory() as dir_name:
            file_name = os.path.join(dir_name, "test_serialise_to_file.json")
            resource = Inherit()
            resource.to_file(file_name)

            # Test that the file was written with appropriate contents
            with open(file_name, "r") as fp:
                serialised = json.load(fp)

            self.assertTrue("id" in serialised.keys())
            self.assertTrue("field_to_serialise" in serialised.keys())
            self.assertFalse("logger" in serialised.keys())
            self.assertFalse("_field_not_to_serialise" in serialised.keys())
