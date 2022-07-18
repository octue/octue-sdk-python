import json
import os
from tempfile import TemporaryDirectory

from octue.mixins import Serialisable
from tests.base import BaseTestCase


class Inherit(Serialisable):
    def __init__(self):
        super().__init__()
        self.id = "id"
        self.field_to_serialise = 0
        self._field_not_to_serialise = 1


class InheritWithFieldsToSerialise(Inherit):
    _SERIALISE_FIELDS = ("field_to_serialise",)


class SerialisableTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments"""
        Serialisable()

    def test_returns_primitive_without_logger_or_protected_fields(self):
        """Ensures class instantiates with a UUID()"""
        resource = Inherit()
        serialised = resource.to_primitive()
        self.assertTrue("id" in serialised.keys())
        self.assertTrue("field_to_serialise" in serialised.keys())
        self.assertFalse("_field_not_to_serialise" in serialised.keys())

    def test_serialise_only_attrs(self):
        """Restricts the id field, which would normally be serialised"""
        resource = InheritWithFieldsToSerialise()
        serialised = resource.to_primitive()
        self.assertFalse("id" in serialised.keys())
        self.assertTrue("field_to_serialise" in serialised.keys())
        self.assertFalse("_field_not_to_serialise" in serialised.keys())

    def test_serialise_to_string(self):
        """Restricts the id field, which would normally be serialised"""
        resource = InheritWithFieldsToSerialise()
        serialised = resource.serialise()
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
            self.assertFalse("_field_not_to_serialise" in serialised.keys())
