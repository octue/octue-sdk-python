import os
import shutil
import sys
import uuid

from octue import Runner
from ..base import BaseTestCase


class TemplateAppsTestCase(BaseTestCase):
    """ Test case that runs analyses using apps in the templates, to ensure all the examples work.
    """

    def setUp(self):
        super().setUp()
        self.start_path = os.getcwd()

        # Initialise so these variables are assigned on the instance
        self.template_data_path = None
        self.template_twine = None
        self.template_path = None
        self.app_test_path = None
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
        analysis = runner.run(
            app_src=self.template_path, output_manifest_path=os.path.join("data", "output", "manifest.json")
        )
        analysis.finalise(output_dir=os.path.join("data", "output"))

    def test_using_manifests(self):
        """ Ensures using-manifests app works correctly
        """
        self.set_template("template-using-manifests")
        runner = Runner(
            twine=self.template_twine, configuration_values=os.path.join("data", "configuration", "values.json"),
        )
        analysis = runner.run(
            app_src=self.template_path,
            input_manifest=os.path.join("data", "input", "manifest.json"),
            output_manifest_path=os.path.join("data", "output", "manifest.json"),
        )
        analysis.finalise(output_dir=os.path.join("data", "output"))
        self.assertTrue(os.path.isfile(os.path.join("data", "output", "cleaned_met_mast_data", "cleaned.csv")))
