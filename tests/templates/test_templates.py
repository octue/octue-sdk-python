import os
import uuid
from tempfile import TemporaryDirectory, gettempdir

from octue import exceptions
from octue.templates import copy_template
from ..base import BaseTestCase


class TemplatesTestCase(BaseTestCase):
    """Test parts of the utils library not otherwise covered by other tests"""

    def setUp(self):
        super().setUp()
        self.start_path = os.getcwd()
        self.test_id = str(uuid.uuid4())
        self.tmp_dir_name = os.path.join(gettempdir(), "octue-sdk-python", f"test-{self.test_id}")

    def tearDown(self):
        """Remove data directories manufactured and ensure we end up in the right place"""
        super().tearDown()
        os.chdir(self.start_path)

    def test_copy_templates_to_directory(self):
        """Ensures that a known template will copy to a given directory"""
        with TemporaryDirectory() as tmp_dir:
            copy_template("template-python-fractal", tmp_dir)
            twine_file_name = os.path.join(tmp_dir, "template-python-fractal", "twine.json")
            self.assertTrue(os.path.isfile(twine_file_name))

    def test_copy_templates_to_current_directory_by_default(self):
        """Ensures that a known template will copy to the current directory by default"""
        with TemporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            copy_template("template-python-fractal")
            self.assertTrue(os.path.isfile(os.path.join(".", "template-python-fractal", "twine.json")))

    def test_copy_template_raises_if_unknown(self):
        """Ensures that a known template will copy to a given directory"""
        with TemporaryDirectory() as tmp_dir:
            with self.assertRaises(exceptions.InvalidInputException) as error:
                copy_template("template-which-isnt-there", tmp_dir)
            self.assertIn("Unknown template name 'template-which-isnt-there', try one of", error.exception.args[0])
