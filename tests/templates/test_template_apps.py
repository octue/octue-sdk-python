import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from octue import Runner
from octue.utils.processes import ProcessesContextManager
from ..base import BaseTestCase


class TemplateAppsTestCase(BaseTestCase):
    """ Test case that runs analyses using apps in the templates, to ensure all the examples work. """

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
            """Sets up the working directory and data paths to run one of the provided templates"""
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
        """Remove data directories manufactured"""
        super().tearDown()
        os.chdir(self.start_path)
        for path in self.teardown_templates:
            sys.path.remove(path)
            shutil.rmtree(path)

    def test_fractal_configuration(self):
        """ Ensures fractal app can be configured with its default configuration. """
        self.set_template("template-python-fractal")
        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "configuration_values.json"),
            output_manifest_path=os.path.join("data", "output", "manifest.json"),
        )

        analysis = runner.run()
        analysis.finalise(output_dir=os.path.join("data", "output"))

    def test_using_manifests(self):
        """ Ensures using-manifests app works correctly. """
        self.set_template("template-using-manifests")
        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "values.json"),
            output_manifest_path=os.path.join("data", "output", "manifest.json"),
        )

        analysis = runner.run(input_manifest=os.path.join("data", "input", "manifest.json"))
        analysis.finalise(output_dir=os.path.join("data", "output"))
        self.assertTrue(os.path.isfile(os.path.join("data", "output", "cleaned_met_mast_data", "cleaned.csv")))

    def test_child_services_template(self):
        """Ensure the child services template works correctly (i.e. that children can be accessed by a parent and data
        collected from them). This template has a parent app and two children - an elevation app and wind speed app. The
        parent sends coordinates to both children, receiving the elevation and wind speed from them at these locations.
        """
        cli_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "octue", "cli.py")
        self.set_template("template-child-services")

        elevation_service_path = os.path.join(self.template_path, "elevation_service")
        elevation_service_uuid = str(uuid.uuid4())
        elevation_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "start",
                f"--app-dir={elevation_service_path}",
                f"--twine={os.path.join(elevation_service_path, 'twine.json')}",
                f"--config-dir={os.path.join(elevation_service_path, 'data', 'configuration')}",
                f"--service-id={elevation_service_uuid}",
                "--delete-topic-and-subscription-on-exit",
            ]
        )

        wind_speed_service_path = os.path.join(self.template_path, "wind_speed_service")
        wind_speed_service_uuid = str(uuid.uuid4())
        wind_speed_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "start",
                f"--app-dir={wind_speed_service_path}",
                f"--twine={os.path.join(wind_speed_service_path, 'twine.json')}",
                f"--config-dir={os.path.join(wind_speed_service_path, 'data', 'configuration')}",
                f"--service-id={wind_speed_service_uuid}",
                "--delete-topic-and-subscription-on-exit",
            ]
        )

        with ProcessesContextManager(processes=(elevation_process, wind_speed_process)):
            parent_service_path = os.path.join(self.template_path, "parent_service")

            # Dynamically alter the UUIDs defined in template children.json file to avoid conflicts when the same tests
            # run in parallel in the GitHub test runner using the actual Google Cloud PubSub instance. Apart from that,
            # the file remains the same so this test tests the template as closely as possible.
            with tempfile.TemporaryDirectory() as temporary_directory:

                with open(os.path.join(parent_service_path, "data", "configuration", "children.json")) as f:
                    template_children = json.load(f)

                template_children[0]["id"] = wind_speed_service_uuid
                template_children[1]["id"] = elevation_service_uuid

                test_children_path = os.path.join(temporary_directory, "children.json")
                with open(test_children_path, "w") as f:
                    json.dump(template_children, f)

                runner = Runner(
                    app_src=parent_service_path,
                    twine=os.path.join(parent_service_path, "twine.json"),
                    children=test_children_path,
                )
                time.sleep(5)
                analysis = runner.run(
                    input_values=os.path.join(parent_service_path, "data", "input", "values.json"),
                )

        analysis.finalise()
        self.assertTrue("elevations" in analysis.output_values)
        self.assertTrue("wind_speeds" in analysis.output_values)
