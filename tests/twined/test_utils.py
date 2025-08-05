from tempfile import TemporaryDirectory

from octue.twined import exceptions
from octue.twined.utils import load_json

from .base import VALID_SCHEMA_TWINE, BaseTestCase


class TestUtils(BaseTestCase):
    """Testing operation of the Twine class"""

    def test_load_json_with_file_like(self):
        """Ensures that json can be loaded from a file-like object"""
        with TemporaryDirectory() as tmp_dir:
            with open(self._write_json_string_to_file(VALID_SCHEMA_TWINE, tmp_dir), "r") as file_like:
                data = load_json(file_like)
                for key in data.keys():
                    self.assertIn(key, ("configuration_values_schema", "input_values_schema", "output_values_schema"))

    def test_load_json_with_object(self):
        """Ensures if load_json is called on an already loaded object, it'll pass-through successfully"""
        already_loaded_data = {"a": 1, "b": 2}
        data = load_json(already_loaded_data)
        for key in data.keys():
            self.assertIn(key, ("a", "b"))

    def test_load_json_with_disallowed_kind(self):
        """Ensures that when attempting to load json with a kind which is diallowed, the correct exception is raised"""
        custom_allowed_kinds = ("file-like", "filename", "object")  # Removed  "string"
        with self.assertRaises(exceptions.InvalidSourceKindException):
            load_json("{}", allowed_kinds=custom_allowed_kinds)
