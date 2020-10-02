import os
import shutil
import sys
import uuid

from octue import Runner
from .base import BaseTestCase


class TemplatesTestCase(BaseTestCase):
    """ Test case that runs the analyses in the templates to ensure all the examples work.
    """

    def setUp(self):
        super().setUp()
        self.start_path = os.getcwd()

        # Initialise just so that pylint picks up these variables are present (reinitialised in set_template())
        self.template_data_path = None
        self.template_twine = None
        self.template_path = None
        self.teardown_templates = []

        def set_template(template):
            """ Sets up the working directory and data paths to run one of the provided templates
            """
            self.template_path = os.path.join(self.templates_path, template)
            self.template_twine = os.path.join(self.templates_path, template, "twine.json")

            # Duplicate the template's data/ directory to a test-specific replica
            self.app_test_path = os.path.join(self.data_path, str(uuid.uuid4()))
            shutil.copytree(self.template_path, self.app_test_path)

            # Add this template to the list to remove in teardown
            self.teardown_templates.append(self.app_test_path)
            sys.path.insert(0, self.app_test_path)

            # Run from within the app folder context
            os.chdir(self.app_test_path)

        self.set_template = set_template

    def tearDown(self):
        """ Remove data directories manufactured
        """
        super().tearDown()
        os.chdir(self.start_path)
        for path in self.teardown_templates:
            sys.path.remove(path)
            shutil.rmtree(path)

    def test_fractal_configuration(self):
        """ Ensures fractal app can be configured with its default configuration
        """
        self.set_template("template-python-fractal")
        runner = Runner(
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "configuration_values.json"),
        )
        from app import run

        runner.run(run)

    def test_fractal_configuration_with_no_config(self):
        """ Ensures fractal app can be configured with no configuration
        """
        runner = Runner(twine=self.template_twine, paths=self.template_data_path)
        runner.configure()

    def test_fractal(self):
        """ Ensures fractal app can be configured with its default configuration
        """
        runner = Runner(twine=self.template_twine, paths=self.template_data_path)
        runner.configure(values=os.path.join(runner.paths["configuration"], "configuration_values.json"))
        runner.run()
