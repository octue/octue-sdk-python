import os
import shutil
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
import uuid

import yaml

from octue import REPOSITORY_ROOT, Runner
from octue.cloud.emulators.cloud_storage import mock_generate_signed_url
from octue.cloud.service_id import create_sruid
from octue.resources.manifest import Manifest
from octue.twined.cloud.emulators import ChildEmulator
from octue.utils.processes import ProcessesContextManager
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME
from tests.base import BaseTestCase


class TemplateAppsTestCase(BaseTestCase):
    """A test case that runs analyses using the template apps to ensure they work."""

    def test_fractal_template_with_default_configuration(self):
        """Ensure the `fractal` app can be configured with its default configuration and run."""
        self.set_template("template-fractal")

        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "configuration_values.json"),
        )

        runner.run()

    def test_using_manifests_template(self):
        """Ensure the `using-manifests` template app works correctly."""
        self.set_template("template-using-manifests")
        output_location = f"gs://{TEST_BUCKET_NAME}"

        runner = Runner(
            app_src=self.template_path,
            twine=self.template_twine,
            configuration_values=os.path.join("data", "configuration", "values.json"),
            output_location=output_location,
        )

        with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
            analysis = runner.run(input_manifest=os.path.join("data", "input", "manifest.json"))

        # Test that the signed URLs for the dataset and its files work and can be used to reinstantiate the output
        # manifest after serialisation.
        downloaded_output_manifest = Manifest.deserialise(analysis.output_manifest.to_primitive())

        self.assertEqual(
            downloaded_output_manifest.datasets["cleaned_met_mast_data"].labels,
            {"mast", "cleaned", "met"},
        )

        output_path = downloaded_output_manifest.datasets["cleaned_met_mast_data"].files.one().cloud_path
        self.assertTrue(output_path.startswith(f"gs://{TEST_BUCKET_NAME}"))
        self.assertTrue(output_path.endswith("/cleaned_met_mast_data/cleaned.csv"))

    @unittest.skipIf(condition=os.name == "nt", reason="See issue https://github.com/octue/octue-sdk-python/issues/229")
    def test_child_services_template(self):
        """Ensure the child services template works correctly (i.e. that children can be accessed by a parent and data
        collected from them). This template has a parent app and two children - an elevation app and wind speed app. The
        parent sends coordinates to both children, receiving the elevation and wind speed from them at these locations.
        """
        cli_path = os.path.join(REPOSITORY_ROOT, "octue", "cli.py")
        self.set_template("template-child-services")

        elevation_service_path = os.path.join(self.template_path, "elevation_service")
        elevation_service_revision_tag = str(uuid.uuid4())

        elevation_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "twined",
                "start",
                f"--service-config={os.path.join(elevation_service_path, 'octue.yaml')}",
            ],
            cwd=elevation_service_path,
            env={**os.environ, "OCTUE_SERVICE_REVISION_TAG": elevation_service_revision_tag},
        )

        wind_speed_service_path = os.path.join(self.template_path, "wind_speed_service")
        wind_speed_service_revision_tag = str(uuid.uuid4())

        wind_speed_process = subprocess.Popen(
            [
                sys.executable,
                cli_path,
                "twined",
                "start",
                f"--service-config={os.path.join(wind_speed_service_path, 'octue.yaml')}",
            ],
            cwd=wind_speed_service_path,
            env={**os.environ, "OCTUE_SERVICE_REVISION_TAG": wind_speed_service_revision_tag},
        )

        parent_service_path = os.path.join(self.template_path, "parent_service")
        namespace = "template-child-services"

        with open(os.path.join(parent_service_path, "octue.yaml")) as f:
            children = yaml.safe_load(f)["services"][0]["children"]

            children[0]["id"] = create_sruid(
                namespace=namespace,
                name="wind-speed-service",
                revision_tag=wind_speed_service_revision_tag,
            )

            children[1]["id"] = create_sruid(
                namespace=namespace,
                name="elevation-service",
                revision_tag=elevation_service_revision_tag,
            )

        with ProcessesContextManager(processes=(elevation_process, wind_speed_process)):
            runner = Runner(
                app_src=parent_service_path,
                twine=os.path.join(parent_service_path, "twine.json"),
                children=children,
                service_id="template-child-services/parent-service:local",
            )

            # Wait for the child service subscriptions to show up as created in Pub/Sub.
            time.sleep(5)

            analysis = runner.run(input_values=os.path.join(parent_service_path, "data", "input", "values.json"))

        self.assertTrue("elevations" in analysis.output_values)
        self.assertTrue("wind_speeds" in analysis.output_values)

    def test_child_services_template_using_emulated_children(self):
        """Test the child services template app using emulated children."""
        self.set_template("template-child-services")
        parent_service_path = os.path.join(self.template_path, "parent_service")

        with open(os.path.join(parent_service_path, "octue.yaml")) as f:
            children = yaml.safe_load(f)["services"][0]["children"]

        runner = Runner(
            app_src=parent_service_path,
            twine=os.path.join(parent_service_path, "twine.json"),
            children=children,
            service_id=f"template-child-services/parent-service:{MOCK_SERVICE_REVISION_TAG}",
        )

        attributes = {
            "datetime": "2024-04-11T10:46:48.236064",
            "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
            "retry_count": 0,
            "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
            "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
            "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
            "parent": "octue/test-service:1.0.0",
            "originator": "octue/test-service:1.0.0",
            "sender": "octue/test-service:1.0.0",
            "sender_type": "CHILD",
            "sender_sdk_version": "0.51.0",
            "recipient": "octue/another-service:3.2.1",
        }

        emulated_children = [
            ChildEmulator(
                events=[
                    {
                        "event": {"kind": "result", "output_values": [10]},
                        "attributes": attributes,
                    }
                ],
            ),
            ChildEmulator(
                events=[
                    {
                        "event": {"kind": "result", "output_values": [300]},
                        "attributes": attributes,
                    },
                ],
            ),
        ]

        with patch("octue.runner.Child", side_effect=emulated_children):
            analysis = runner.run(input_values=os.path.join(parent_service_path, "data", "input", "values.json"))

        self.assertEqual(analysis.output_values, {"wind_speeds": [10], "elevations": [300]})

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
        """Remove the temporary template app directories."""
        super().tearDown()
        os.chdir(self.start_path)
        for path in self.teardown_templates:
            sys.path.remove(path)
            shutil.rmtree(path)
