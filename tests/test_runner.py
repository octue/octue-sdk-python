import os
from unittest.mock import Mock, patch

import twined
from octue import Runner
from .base import BaseTestCase


def mock_app(analysis):
    pass


class RunnerTestCase(BaseTestCase):
    def test_instantiate_runner(self):
        """Ensures that runner whose twine requires configuration can be instantiated"""
        runner = Runner(app_src=".", twine="{}")
        self.assertEqual(runner.__class__.__name__, "Runner")

    def test_run_with_configuration_passes(self):
        """Ensures that runs can be made with configuration only"""
        runner = Runner(
            app_src=mock_app,
            twine="""{
            "configuration_values_schema": {
                "type": "object",
                "properties": {
                    "n_iterations": {
                        "type": "integer"
                    }
                }
            }
        }""",
            configuration_values="{}",
        )

        runner.run()

    def test_instantiation_without_configuration_fails(self):
        """Ensures that runner can be instantiated with a string that points to a path"""
        with self.assertRaises(twined.exceptions.TwineValueException) as error:
            Runner(
                app_src=".",
                twine="""{
                "configuration_values_schema": {
                    "type": "object",
                    "properties": {
                        "n_iterations": {
                            "type": "integer"
                        }
                    }
                }
            }""",
            )

        self.assertIn(
            "The 'configuration_values' strand is defined in the twine, but no data is provided in sources",
            error.exception.args[0],
        )

    def test_run_output_values_validation(self):
        """Ensures that runner can be instantiated with a string that points to a path"""
        twine = """
            {
                "output_values_schema": {
                    "type": "object",
                    "required": ["n_iterations"],
                    "properties": {
                        "n_iterations": {
                            "type": "integer"
                        }
                    }
                }
            }

        """
        runner = Runner(app_src=mock_app, twine=twine)

        # Test for failure with an incorrect output
        with self.assertRaises(twined.exceptions.InvalidValuesContents) as error:
            runner.run().finalise()

        self.assertIn("'n_iterations' is a required property", error.exception.args[0])

        # Test for success with a valid output
        def fcn(analysis):
            analysis.output_values["n_iterations"] = 10

        Runner(app_src=fcn, twine=twine).run().finalise()

    def test_exception_raised_when_strand_data_missing(self):
        """Ensures that protected attributes can't be set"""
        runner = Runner(
            app_src=mock_app,
            twine="""{
                "configuration_values_schema": {
                    "type": "object",
                    "properties": {
                        "n_iterations": {
                            "type": "integer"
                        }
                    }
                },
                "input_values_schema": {
                    "type": "object",
                    "properties": {
                        "height": {
                            "type": "integer"
                        }
                    },
                    "required": ["height"]
                }
            }""",
            configuration_values={"n_iterations": 5},
        )

        with self.assertRaises(twined.exceptions.TwineValueException) as error:
            runner.run()

        self.assertIn(
            "The 'input_values' strand is defined in the twine, but no data is provided in sources",
            error.exception.args[0],
        )

    def test_output_manifest_is_not_none(self):
        """ Ensure the output manifest of an analysis is not None if an output manifest is defined in the Twine. """
        runner = Runner(
            app_src=mock_app,
            twine="""
                {
                    "output_manifest": [
                        {
                            "key": "open_foam_result",
                            "purpose": "A dataset containing solution fields of an openfoam case."
                        },
                        {
                            "key": "airfoil_cp_values",
                            "purpose": "A file containing cp values"
                        }
                    ]
                }
            """,
        )

        analysis = runner.run()
        self.assertIsNotNone(analysis.output_manifest)

    def test_runner_with_credentials(self):
        """Test that credentials can be found locally and populated into the environment from Google Cloud Secret
        Manager.
        """
        os.environ["SECRET_THE_FIRST"] = "my-secret"

        runner = Runner(
            app_src=mock_app,
            twine="""
                {
                    "credentials": [
                        {
                            "name": "SECRET_THE_FIRST",
                            "purpose": "Token for accessing a 3rd party API service",
                            "location": "local"
                        },
                        {
                            "name": "TEST_SECRET",
                            "purpose": "Token for accessing a 3rd party API service",
                            "location": "local"
                        }
                    ]
                }
            """,
            credentials="""
                [
                    {"name": "SECRET_THE_FIRST"},
                    {
                        "name": "TEST_SECRET",
                        "location": "google",
                        "project_name": "windquest",
                        "version": "latest"
                    }
                ]
            """,
        )

        class MockAccessSecretVersionResponse:
            payload = Mock()
            payload.data = b"My precious!"

        # An error will be raised if secret validation fails.
        with patch(
            "google.cloud.secretmanager_v1.services.secret_manager_service.client.SecretManagerServiceClient"
            ".access_secret_version",
            return_value=MockAccessSecretVersionResponse(),
        ):
            runner.run()

        # Check that first secret is still present and that the Google Cloud secret is now in the environment.
        self.assertEqual(os.environ["SECRET_THE_FIRST"], "my-secret")
        self.assertEqual(os.environ["TEST_SECRET"], "My precious!")

    def test_invalid_app_directory(self):
        """ Ensure an error containing the searched location is raised if the app source can't be found. """
        runner = Runner(app_src="..", twine="{}")

        with self.assertRaises(ModuleNotFoundError) as e:
            runner.run()
            self.assertTrue("No module named 'app'" in e.msg)
            self.assertTrue(os.path.abspath(runner.app_src) in e.msg)
