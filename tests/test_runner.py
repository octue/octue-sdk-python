import copy
import os
from unittest.mock import Mock, patch

from jsonschema.validators import RefResolver

import twined
from octue import Runner
from tests import TESTS_DIR
from tests.base import BaseTestCase
from tests.test_app_modules.app_class.app import App
from tests.test_app_modules.app_module import app


def mock_app(analysis):
    """Run a mock analysis that does nothing.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """
    pass


class TestRunner(BaseTestCase):
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
        with self.assertRaises(twined.exceptions.TwineValueException):
            runner.run().finalise()

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
        """Ensure the output manifest of an analysis is not None if an output manifest is defined in the Twine."""
        runner = Runner(
            app_src=mock_app,
            twine="""
                {
                    "output_manifest": {
                        "datasets": {
                            "open_foam_result": {
                                "purpose": "A dataset containing solution fields of an openfoam case."
                            },
                            "airfoil_cp_values": {
                                "purpose": "A file containing cp values"
                            }
                        }
                    }
                }
            """,
        )

        analysis = runner.run()
        self.assertIsNotNone(analysis.output_manifest)

    def test_runner_with_credentials(self):
        """Test that credentials can be used with Runner."""
        with patch.dict(os.environ, {"LOCAL_CREDENTIAL": "my-secret"}):
            runner = Runner(
                app_src=mock_app,
                twine="""
                    {
                        "credentials": [
                            {
                                "name": "LOCAL_CREDENTIAL",
                                "purpose": "Token for accessing a 3rd party API service"
                            }
                        ]
                    }
                """,
            )

            runner.run()
            self.assertEqual(os.environ["LOCAL_CREDENTIAL"], "my-secret")

    def test_runner_with_google_secret_credentials(self):
        """Test that credentials can be found locally and populated into the environment from Google Cloud Secret
        Manager.
        """
        with patch.dict(os.environ, {"LOCAL_CREDENTIAL": "my-secret"}):

            runner = Runner(
                app_src=mock_app,
                twine="""
                    {
                        "credentials": [
                            {
                                "name": "LOCAL_CREDENTIAL",
                                "purpose": "Token for accessing a 3rd party API service"
                            },
                            {
                                "name": "CLOUD_CREDENTIALS",
                                "purpose": "Token for accessing another 3rd party API service"
                            }
                        ]
                    }
                """,
            )

            class MockAccessSecretVersionResponse:
                payload = Mock()
                payload.data = b"My precious!"

            with patch(
                "google.cloud.secretmanager_v1.services.secret_manager_service.client.SecretManagerServiceClient"
                ".access_secret_version",
                return_value=MockAccessSecretVersionResponse(),
            ):
                runner.run()

            # Check that first secret is still present and that the Google Cloud secret is now in the environment.
            self.assertEqual(os.environ["LOCAL_CREDENTIAL"], "my-secret")
            self.assertEqual(os.environ["CLOUD_CREDENTIALS"], "My precious!")

    def test_invalid_app_directory(self):
        """Ensure an error containing the searched location is raised if the app source can't be found."""
        runner = Runner(app_src="..", twine="{}")

        with self.assertRaises(ModuleNotFoundError) as e:
            runner.run()
            self.assertTrue("No module named 'app'" in e.msg)
            self.assertTrue(os.path.abspath(runner.app_src) in e.msg)

    def test_app_can_be_provided_as_a_class(self):
        """Test that apps can be written and provided as a class."""
        analysis = Runner(app_src=App, twine="{}").run()
        self.assertEqual(analysis.output_values, "App as a class works!")

    def test_app_can_be_provided_as_path_to_module_containing_class_named_app(self):
        """Test that apps can be provided as a path to a module containing a class named "App"."""
        analysis = Runner(app_src=os.path.join(TESTS_DIR, "test_app_modules", "app_class"), twine="{}").run()
        self.assertEqual(analysis.output_values, "App as a class works!")

    def test_app_can_be_provided_as_a_module_containing_function_named_run(self):
        """Test that apps can be provided as a module containing a function named "run"."""
        analysis = Runner(app_src=app, twine="{}").run()
        self.assertEqual(analysis.output_values, "App as a module works!")


class TestRunnerWithRequiredDatasetFileTags(BaseTestCase):

    TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE = """
        {
            "input_manifest": {
                "datasets": {
                    "met_mast_data": {
                        "purpose": "A dataset containing meteorological mast data",
                        "file_tags_template": {
                            "type": "object",
                            "properties": {
                                "manufacturer": {
                                    "type": "string"
                                },
                                "height": {
                                    "type": "number"
                                },
                                "is_recycled": {
                                    "type": "boolean"
                                },
                                "number_of_blades": {
                                    "type": "number"
                                }
                            },
                            "required": [
                                "manufacturer",
                                "height",
                                "is_recycled",
                                "number_of_blades"
                            ]
                        }
                    }
                }
            }
        }
    """

    INPUT_MANIFEST_WITH_CORRECT_FILE_TAGS = """
        {
            "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
            "datasets": {
                "met_mast_data": {
                    "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
                    "name": "met_mast_data",
                    "tags": {},
                    "labels": ["met", "mast", "wind"],
                    "files": [
                        {
                            "path": "input/datasets/7ead7669/file_1.csv",
                            "cluster": 0,
                            "sequence": 0,
                            "extension": "csv",
                            "labels": ["mykeyword1", "mykeyword2"],
                            "tags": {
                                "manufacturer": "vestas",
                                "height": 500,
                                "is_recycled": true,
                                "number_of_blades": 3
                            },
                            "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
                            "name": "file_1.csv"
                        },
                        {
                            "path": "input/datasets/7ead7669/file_1.csv",
                            "cluster": 0,
                            "sequence": 1,
                            "extension": "csv",
                            "labels": [],
                            "tags": {
                                "manufacturer": "vestas",
                                "height": 500,
                                "is_recycled": true,
                                "number_of_blades": 3
                            },
                            "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
                            "name": "file_1.csv"
                        }
                    ]
                }
            }
        }
    """

    def test_error_raised_when_required_tags_missing_for_validate_input_manifest(self):
        """Test that an error is raised when required tags from the file tags template for a dataset are missing when
        validating the input manifest.
        """
        input_manifest = """
            {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "met_mast_data": {
                        "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
                        "name": "met_mast_data",
                        "tags": {},
                        "labels": ["met", "mast", "wind"],
                        "files": [
                            {
                                "path": "input/datasets/7ead7669/file_1.csv",
                                "cluster": 0,
                                "sequence": 0,
                                "extension": "csv",
                                "tags": {},
                                "labels": [],
                                "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
                                "name": "file_1.csv"
                            }
                        ]
                    }
                }
            }
        """

        runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)

        with self.assertRaises(twined.exceptions.InvalidManifestContents):
            runner.run(input_manifest=input_manifest)

    def test_validate_input_manifest_raises_error_if_required_tags_are_not_of_required_type(self):
        """Test that an error is raised if the required tags from the file tags template for a dataset are present but
        are not of the required type when validating an input manifest.
        """
        input_manifest = """
            {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "met_mast_data": {
                        "id": "7ead7669-8162-4f64-8cd5-4abe92509e17",
                        "name": "met_mast_data",
                        "tags": {},
                        "labels": ["met", "mast", "wind"],
                        "files": [
                            {
                                "path": "input/datasets/7ead7669/file_1.csv",
                                "cluster": 0,
                                "sequence": 0,
                                "extension": "csv",
                                "tags": %s,
                                "labels": [],
                                "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
                                "name": "file_1.csv"
                            }
                        ]
                    }
                }
            }
        """

        runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)

        for tags in (
            '{"manufacturer": "Vestas", "height": 350, "is_recycled": false, "number_of_blades": "3"}',
            '{"manufacturer": "Vestas", "height": 350, "is_recycled": "no", "number_of_blades": 3}',
            '{"manufacturer": false, "height": 350, "is_recycled": "false", "number_of_blades": 3}',
        ):
            with self.subTest(tags=tags):
                with self.assertRaises(twined.exceptions.InvalidManifestContents):
                    runner.run(input_manifest=input_manifest % tags)

    def test_validate_input_manifest_with_required_tags(self):
        """Test that validating an input manifest with required tags from the file tags template for a dataset works
        for tags meeting the requirements.
        """
        runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)
        runner.run(input_manifest=self.INPUT_MANIFEST_WITH_CORRECT_FILE_TAGS)

    def test_validate_input_manifest_with_required_tags_for_remote_tag_template_schema(self):
        """Test that a remote tag template can be used for validating tags on the datafiles in a manifest."""
        schema_url = "https://refs.schema.octue.com/octue/my-file-type-tag-template/0.0.0.json"

        twine_with_input_manifest_with_remote_tag_template = (
            """
            {
                "input_manifest": {
                    "datasets": {
                        "met_mast_data": {
                            "purpose": "A dataset containing meteorological mast data",
                            "file_tags_template": {
                                "$ref": "%s"
                            }
                        }
                    }
                }
            }
            """
            % schema_url
        )

        remote_schema = {
            "type": "object",
            "properties": {
                "manufacturer": {"type": "string"},
                "height": {"type": "number"},
                "is_recycled": {"type": "boolean"},
            },
            "required": ["manufacturer", "height", "is_recycled"],
        }

        runner = Runner(app_src=app, twine=twine_with_input_manifest_with_remote_tag_template)
        original_resolve_from_url = copy.copy(RefResolver.resolve_from_url)

        def patch_if_url_is_schema_url(instance, url):
            """Patch the jsonschema validator `RefResolver.resolve_from_url` if the url is the schema URL, otherwise
            leave it unpatched.

            :param jsonschema.validators.RefResolver instance:
            :param str url:
            :return mixed:
            """
            if url == schema_url:
                return remote_schema
            else:
                return original_resolve_from_url(instance, url)

        with patch("jsonschema.validators.RefResolver.resolve_from_url", new=patch_if_url_is_schema_url):
            runner.run(input_manifest=self.INPUT_MANIFEST_WITH_CORRECT_FILE_TAGS)

    def test_validate_input_manifest_with_required_tags_in_several_datasets(self):
        """Test that required tags from the file tags template are validated separately and correctly for each dataset."""
        twine_with_input_manifest_with_required_tags_for_multiple_datasets = """
            {
                "input_manifest": {
                    "datasets": {
                        "first_dataset": {
                            "purpose": "A dataset containing meteorological mast data",
                            "file_tags_template": {
                                "type": "object",
                                "properties": {
                                    "manufacturer": {
                                        "type": "string"
                                    },
                                    "height": {
                                        "type": "number"
                                    }
                                }
                            }
                        },
                        "second_dataset": {
                            "file_tags_template": {
                                "type": "object",
                                "properties": {
                                    "is_recycled": {
                                        "type": "boolean"
                                    },
                                    "number_of_blades": {
                                        "type": "number"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """

        input_manifest = """
            {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "first_dataset": {
                        "id": "7ead7669-8162-4f64-8cd5-4abe92509e19",
                        "name": "first_dataset",
                        "tags": {},
                        "labels": [],
                        "files": [
                            {
                                "path": "input/datasets/7ead7669/file_0.csv",
                                "cluster": 0,
                                "sequence": 0,
                                "extension": "csv",
                                "tags": {
                                    "manufacturer": "Vestas",
                                    "height": 503.7
                                },
                                "labels": [],
                                "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e86",
                                "name": "file_0.csv"
                            }
                        ]
                    },
                    "second_dataset": {
                        "id": "7ead7669-8162-4f64-8cd5-4abe92509e18",
                        "name": "second_dataset",
                        "tags": {},
                        "labels": [],
                        "files": [
                            {
                                "path": "input/datasets/blah/file_1.csv",
                                "cluster": 0,
                                "sequence": 0,
                                "extension": "csv",
                                "tags": {
                                    "is_recycled": true,
                                    "number_of_blades": 3
                                },
                                "labels": [],
                                "id": "abff07bc-7c19-4ed5-be6d-a6546eae8e82",
                                "name": "file_1.csv"
                            }
                        ]
                    }
                }
            }
        """

        runner = Runner(app_src=app, twine=twine_with_input_manifest_with_required_tags_for_multiple_datasets)
        runner.run(input_manifest=input_manifest)
