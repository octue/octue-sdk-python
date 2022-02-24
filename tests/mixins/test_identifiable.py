import uuid

from octue import exceptions
from octue.mixins import Identifiable

from ..base import BaseTestCase


class IdentifiableTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments"""
        resource = Identifiable()
        self.assertIsInstance(resource.id, str)
        self.assertEqual(len(resource.id), 36)

    def test_instantiates_with_str_uuid(self):
        """Ensures class instantiates with a string uuid"""
        resource = Identifiable(id=str(uuid.uuid4()))
        self.assertIsInstance(resource.id, str)
        self.assertEqual(len(resource.id), 36)

    def test_instantiates_with_uuid(self):
        """Ensures class instantiates with a UUID()"""
        resource = Identifiable(id=uuid.uuid4())
        self.assertIsInstance(resource.id, str)
        self.assertEqual(len(resource.id), 36)

    def test_repr_with_no_name(self):
        """Test that the representation uses the ID if no name is provided."""
        resource = Identifiable(id="07d38e81-6b00-4079-901b-e250ea3c7773")
        self.assertEqual(repr(resource), f"<Identifiable({resource.id})>")

    def test_repr_with_name(self):
        """Test that the representation uses the name if one is provided."""
        resource = Identifiable(id="07d38e81-6b00-4079-901b-e250ea3c7773", name="my-name")
        self.assertEqual(repr(resource), "<Identifiable('my-name')>")

    def test_raises_error_with_non_uuid(self):
        """Ensures that if a string is passed not matching the UUID pattern, that an exception is raised"""
        with self.assertRaises(exceptions.InvalidInputException) as e:
            Identifiable(id="notauuid-6b00-4079-901b-e250ea3c7773")

        self.assertIn("is not a valid uuid string or instance of class UUID", e.exception.args[0])

    def test_raises_error_with_non_str_or_uuid(self):
        """Ensures that if an id is passed, it must be of type str or UUID"""

        class NotStrOrUUID:
            pass

        with self.assertRaises(exceptions.InvalidInputException) as e:
            Identifiable(id=NotStrOrUUID())

        self.assertIn("must be a valid uuid string, an instance of class UUID or None", e.exception.args[0])

    def test_str_on_subclass(self):
        """Ensures that calling `str` on an object inheriting from `Identifiable` will format it with the class name and
        ID.
        """

        class Inherit(Identifiable):
            pass

        resource = Inherit(id="07d38e81-6b00-4079-901b-e250ea3c7773")
        self.assertEqual(str(resource), "<Inherit(07d38e81-6b00-4079-901b-e250ea3c7773)>")

    def test_set_id_on_instantiated_object(self):
        """Ensures that setting id on an instantiated object will raise an error message, with a customised classname
        for inheritance
        """

        class Inherit(Identifiable):
            pass

        resource = Inherit()
        with self.assertRaises(AttributeError) as e:
            resource.id = "07d38e81-6b00-4079-901b-e250ea3c7773"

        self.assertIn("can't set attribute", e.exception.args[0])
