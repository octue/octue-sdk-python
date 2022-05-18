import json
import os
import shutil
import subprocess
import sys
import unittest
import uuid
from unittest.mock import patch
from urllib.parse import urlparse

from octue import REPOSITORY_ROOT, Runner
from octue.cloud.emulators import mock_generate_signed_url
from octue.resources.manifest import Manifest
from octue.utils.processes import ProcessesContextManager
from tests.base import BaseTestCase


class TemplateAppsTestCase(BaseTestCase):
    """Test case that runs analyses using apps in the templates, to ensure all the examples work."""

    def setUp(self):
        """Set up the test case by adding empty attributes ready to store details about the templates.

        :return None:
        """
        super().setUp()
        self.start_path = os.getcwd()

        # Initialise so these variables are assigned on the instance
        self.template_twine = None
        self.template_path = None
        self.app_test_path = None
        self.teardown_templates = []

        def set_template(template):
            """Set up the working directory and data paths to run one of the provided templates."""
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
        """Ensures fractal app can be configured with its default configuration."""
        self.set_template("template-python-fractal")
        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "configuration_values.json"),
        )

        runner.run()

    def test_using_manifests(self):
        """Ensure the `using-manifests` template app works correctly."""
        self.set_template("template-using-manifests")

        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "values.json"),
        )

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
            analysis = runner.run(input_manifest=os.path.join("data", "input", "manifest.json"))

        # Check that the output files have been created.
        self.assertTrue(os.path.isfile(os.path.join("cleaned_met_mast_data", "cleaned.csv")))

        # Test that the signed URLs for the dataset and its files work and can be used to reinstantiate the output
        # manifest after serialisation.
        downloaded_output_manifest = Manifest.deserialise(analysis.output_manifest.to_primitive())

        self.assertEqual(
            downloaded_output_manifest.datasets["cleaned_met_mast_data"].labels,
            {"mast", "cleaned", "met"},
        )

        self.assertEqual(
            urlparse(downloaded_output_manifest.datasets["cleaned_met_mast_data"].files.one().cloud_path).path,
            "/octue-test-bucket/output/test_using_manifests_analysis/cleaned_met_mast_data/cleaned.csv",
        )

    @unittest.skipIf(condition=os.name == "nt", reason="See issue https://github.com/octue/octue-sdk-python/issues/229")
    def test_child_services_template(self):
        """Ensure the child services template works correctly (i.e. that children can be accessed by a parent and data
        collected from them). This template has a parent app and two children - an elevation app and wind speed app. The
        parent sends coordinates to both children, receiving the elevation and wind speed from them at these locations.
        """
        cli_path = os.path.join(REPOSITORY_ROOT, "octue", "cli.py")
        self.set_template("template-child-services")

        elevation_service_path = os.path.join(self.template_path, "elevation_service")

        elevation_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "start",
                "--service-configuration-path=octue.yaml",
                "--delete-topic-and-subscription-on-exit",
            ],
            cwd=elevation_service_path,
        )

        wind_speed_service_path = os.path.join(self.template_path, "wind_speed_service")

        wind_speed_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "start",
                "--service-configuration-path=octue.yaml",
                "--delete-topic-and-subscription-on-exit",
            ],
            cwd=wind_speed_service_path,
        )

        parent_service_path = os.path.join(self.template_path, "parent_service")

        with open(os.path.join(parent_service_path, "app_configuration.json")) as f:
            children = json.load(f)["children"]

        with ProcessesContextManager(processes=(elevation_process, wind_speed_process)):

            runner = Runner(
                app_src=parent_service_path,
                twine=os.path.join(parent_service_path, "twine.json"),
                children=children,
            )

            analysis = runner.run(
                input_values=os.path.join(parent_service_path, "data", "input", "values.json"),
            )

        self.assertTrue("elevations" in analysis.output_values)
        self.assertTrue("wind_speeds" in analysis.output_values)
