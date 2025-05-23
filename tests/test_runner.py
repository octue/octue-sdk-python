import json
import logging
import os
import random
import tempfile
import time
import uuid
from unittest.mock import Mock, patch

import coolname

import twined
from octue import Runner, exceptions
from octue.cloud import storage
from octue.cloud.emulators import ChildEmulator
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Dataset, Manifest
from octue.resources.datafile import Datafile
from octue.utils.files import RegisteredTemporaryDirectory, registered_temporary_directories
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME, TESTS_DIR
from tests.base import BaseTestCase
from tests.test_app_modules.app_class.app import App
from tests.test_app_modules.app_module import app


with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
    EVENTS = json.load(f)


ATTRIBUTES = {
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

    def test_repr(self):
        """Test that runners are represented as a string correctly."""
        runner = Runner(app_src=".", twine="{}", service_id=f"octue/my-service:{MOCK_SERVICE_REVISION_TAG}")
        self.assertEqual(repr(runner), f"<Runner('octue/my-service:{MOCK_SERVICE_REVISION_TAG}')>")

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
            self.assertTrue(os.path.abspath(runner.app_source) in e.msg)

    def test_app_can_be_provided_as_a_class(self):
        """Test that apps can be written and provided as a class."""
        analysis = Runner(app_src=App, twine={"output_values_schema": {}}).run()
        self.assertEqual(analysis.output_values, "App as a class works!")

    def test_app_can_be_provided_as_path_to_module_containing_class_named_app(self):
        """Test that apps can be provided as a path to a module containing a class named "App"."""
        analysis = Runner(
            app_src=os.path.join(TESTS_DIR, "test_app_modules", "app_class"),
            twine={"output_values_schema": {}},
        ).run()

        self.assertEqual(analysis.output_values, "App as a class works!")

    def test_app_can_be_provided_as_a_module_containing_function_named_run(self):
        """Test that apps can be provided as a module containing a function named "run"."""
        analysis = Runner(app_src=app, twine={"output_values_schema": {}}).run()
        self.assertEqual(analysis.output_values, {"width": 3})

    def test_app_submodule_logs_are_handled(self):
        """Test that log messages from modules imported into an app are handled and capturable."""
        runner = Runner(
            app_src=os.path.join(TESTS_DIR, "test_app_modules", "app_using_submodule"),
            twine={},
        )

        analysis_id = "0add0168-48ea-48ba-8b82-1bfa24ee8c28"

        with self.assertLogs(level=logging.INFO) as logging_context:
            runner.run(analysis_id=analysis_id)

        self.assertIn(analysis_id, logging_context.output[0])
        self.assertEqual(logging_context.records[0].name, "app")
        self.assertEqual(logging_context.records[0].message, "Log message from app.")

        self.assertIn(analysis_id, logging_context.output[1])
        self.assertEqual(logging_context.records[1].name, "tests.test_app_modules.app_using_submodule.submodule")
        self.assertEqual(logging_context.records[1].message, "Log message from submodule.")

    def test_analysis_finalised_by_runner_if_not_finalised_in_app(self):
        """Test that analyses are finalised automatically if they're not finalised within their app."""
        analysis = Runner(app_src=App, twine={"output_values_schema": {}}).run()
        self.assertTrue(analysis.finalised)

    def test_analysis_not_re_finalised_by_runner_if_finalised_in_app(self):
        """Test that the `Analysis.finalise` method is not called again if an analysis has already been finalised."""

        def app(analysis):
            analysis.output_values = {"hello": "world"}

            self.assertFalse(analysis.finalised)

            # Simulate the analysis being finalised while still being able to mock `Analysis.finalise` to count how
            # many times it's been called.
            analysis._finalised = True

        with patch("octue.resources.analysis.Analysis.finalise") as mock_finalise:
            analysis = Runner(app_src=app, twine={"output_values_schema": {}}).run()

        self.assertEqual(mock_finalise.call_count, 0)
        self.assertTrue(analysis.finalised)

    def test_child_events_saved_even_if_child_ask_method_raises_error(self):
        """Test that events from the child are still saved even if an error is raised within the `Child.ask` method."""
        diagnostics_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "diagnostics")

        def app(analysis):
            analysis.children["my-child"].ask(input_values=[1, 2, 3, 4])
            analysis.children["another-child"].ask(input_values="miaow")

        runner = Runner(
            app_src=app,
            twine={
                "children": [
                    {"key": "my-child"},
                    {"key": "another-child"},
                ],
                "input_values_schema": {},
            },
            children=[
                {
                    "key": "my-child",
                    "id": f"octue/the-child:{MOCK_SERVICE_REVISION_TAG}",
                    "backend": {
                        "name": "GCPPubSubBackend",
                        "project_id": "my-project",
                    },
                },
                {
                    "key": "another-child",
                    "id": f"octue/yet-another-child:{MOCK_SERVICE_REVISION_TAG}",
                    "backend": {
                        "name": "GCPPubSubBackend",
                        "project_id": "my-project",
                    },
                },
            ],
            diagnostics_cloud_path=diagnostics_cloud_path,
            service_id="octue/my-app:2.5.8",
        )

        emulated_children = [
            ChildEmulator(events=EVENTS),
            ChildEmulator(
                events=[
                    *EVENTS[:-2],
                    {
                        "event": {
                            "kind": "exception",
                            "exception_type": "TypeError",
                            "exception_message": "This simulates an error in the child.",
                            "exception_traceback": [""],
                        },
                        "attributes": ATTRIBUTES,
                    },
                ],
            ),
        ]

        analysis_id = "4b91e3f0-4492-49e3-8061-34f1942dc68a"

        # Run the app.
        with patch("octue.runner.Child", side_effect=emulated_children):
            with self.assertRaises(TypeError):
                runner.run(analysis_id=analysis_id, input_values={"hello": "world"})

        storage_client = GoogleCloudStorageClient()
        question_diagnostics_path = storage.path.join(diagnostics_cloud_path, analysis_id)

        # Check the input values.
        self.assertEqual(
            storage_client.download_as_string(storage.path.join(question_diagnostics_path, "input_values.json")),
            json.dumps({"hello": "world"}),
        )

        # Check that events from the children have been recorded.
        with Datafile(storage.path.join(question_diagnostics_path, "questions.json")) as (_, f):
            questions = json.load(f)

        # First question.
        self.assertEqual(questions[0]["key"], "my-child")
        self.assertEqual(questions[0]["input_values"], [1, 2, 3, 4])
        self.assertEqual(len(questions[0]["events"]), 8)

        # Second question.
        self.assertEqual(questions[1]["key"], "another-child")
        self.assertEqual(questions[1]["input_values"], "miaow")

        exception = questions[1]["events"][-1]["event"]
        self.assertEqual(exception["kind"], "exception")
        self.assertEqual(exception["exception_type"], "TypeError")
        self.assertEqual(exception["exception_message"], "This simulates an error in the child.")

    def test_set_up_periodic_monitor_messages(self):
        """Test that multilple periodic monitor messages can be set up from an analysis."""
        monitor_messages = []

        def app(analysis):
            analysis.set_up_periodic_monitor_message(
                create_monitor_message=lambda: {"random_integer": random.randint(0, 10000)},
                period=0.05,
            )

            analysis.set_up_periodic_monitor_message(
                create_monitor_message=lambda: {"random_word": coolname.generate_slug(2)},
                period=0.01,
            )

            time.sleep(0.5)
            analysis.output_values = {"The": "output"}

        runner = Runner(app_src=app, twine={"monitor_message_schema": {}, "output_values_schema": {}})
        analysis = runner.run(handle_monitor_message=monitor_messages.append)
        self.assertEqual(analysis.output_values, {"The": "output"})

        # Check that messages have been sent and that the data is different each time.
        self.assertTrue(len(monitor_messages) > 2)

        # Check that both types of monitor message were sent.
        monitor_message_types = {list(message.keys())[0] for message in monitor_messages}
        self.assertEqual(monitor_message_types, {"random_word", "random_integer"})

        # Check that the periodic monitor message thread has been stopped.
        time.sleep(0.5)

        for thread in analysis._periodic_monitor_message_sender_threads:
            self.assertFalse(thread.is_alive())

    def test_error_raised_if_output_location_invalid(self):
        """Test that an error is raised if an invalid output location is given."""
        with self.assertRaises(exceptions.InvalidInputException):
            Runner(".", twine="{}", output_location="not_a_cloud_path")

    def test_valid_output_location(self):
        """Test that a valid cloud path passes output location validation."""
        Runner(".", twine="{}", output_location="gs://my-bucket/blah")

    def test_downloaded_datafiles_and_registered_temporary_directories_are_deleted_when_runner_finishes(self):
        """Test that datafiles downloaded and registered temporary directories created during an analysis are deleted
        when the runner finishes.
        """
        twine = {
            "output_values_schema": {
                "type": "object",
                "properties": {"downloaded_file_path": {"type": "string"}},
                "required": ["downloaded_file_path"],
            }
        }

        cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "some-file.txt")
        GoogleCloudStorageClient().upload_from_string(string="Aaaaaaaaa", cloud_path=cloud_path)

        def app_that_downloads_datafile(analysis):
            datafile = Datafile(cloud_path)

            # Download a datafile locally.
            datafile.download()

            # Create a temporary directory.
            temporary_directory = RegisteredTemporaryDirectory()
            self.assertTrue(os.path.exists(temporary_directory.name))

            analysis.output_values = {"downloaded_file_path": datafile.local_path}

        analysis = Runner(app_src=app_that_downloads_datafile, twine=twine, delete_local_files=True).run()

        self.assertFalse(os.path.exists(analysis.output_values["downloaded_file_path"]))
        self.assertFalse(os.path.exists(registered_temporary_directories[0].name))


class TestRunnerWithRequiredDatasetFileTags(BaseTestCase):
    TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE = json.dumps(
        {
            "input_manifest": {
                "datasets": {
                    "met_mast_data": {
                        "purpose": "A dataset containing meteorological mast data",
                        "file_tags_template": {
                            "type": "object",
                            "properties": {
                                "manufacturer": {
                                    "type": "string",
                                },
                                "height": {
                                    "type": "number",
                                },
                                "is_recycled": {
                                    "type": "boolean",
                                },
                                "number_of_blades": {
                                    "type": "number",
                                },
                            },
                            "required": [
                                "manufacturer",
                                "height",
                                "is_recycled",
                                "number_of_blades",
                            ],
                        },
                    }
                }
            },
            "output_values_schema": {},
        }
    )

    def test_error_raised_when_required_tags_missing_for_validate_input_manifest(self):
        """Test that an error is raised when required tags from the file tags template for a dataset are missing when
        validating the input manifest.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_path = os.path.join(temporary_directory, "met_mast_data")

            # Make a datafile with no tags.
            with Datafile(os.path.join(dataset_path, "my_file_0.txt"), mode="w") as (datafile, f):
                f.write("hello")

            input_manifest = {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "met_mast_data": dataset_path,
                },
            }

            runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)

            with self.assertRaises(twined.exceptions.InvalidManifestContents):
                runner.run(input_manifest=input_manifest)

    def test_validate_input_manifest_raises_error_if_required_tags_are_not_of_required_type(self):
        """Test that an error is raised if the required tags from the file tags template for a dataset are present but
        are not of the required type when validating an input manifest.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_path = os.path.join(temporary_directory, "met_mast_data")

            input_manifest = {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "met_mast_data": dataset_path,
                },
            }

            runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)

            for tags in (
                {"manufacturer": "Vestas", "height": 350, "is_recycled": False, "number_of_blades": "3"},
                {"manufacturer": "Vestas", "height": 350, "is_recycled": "no", "number_of_blades": 3},
                {"manufacturer": False, "height": 350, "is_recycled": "false", "number_of_blades": 3},
            ):
                with self.subTest(tags=tags):
                    # Make a datafile with the given tags.
                    with Datafile(
                        path=os.path.join(dataset_path, "my_file_0.txt"),
                        tags=tags,
                        mode="w",
                    ) as (datafile, f):
                        f.write("hello")

                    with self.assertRaises(twined.exceptions.InvalidManifestContents):
                        runner.run(input_manifest=input_manifest)

    def test_validate_input_manifest_with_required_tags(self):
        """Test that validating an input manifest with required tags from the file tags template for a dataset works
        for tags meeting the requirements.
        """
        runner = Runner(app_src=app, twine=self.TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE)

        with tempfile.TemporaryDirectory() as temporary_directory:
            input_manifest = self._make_serialised_input_manifest_with_correct_dataset_file_tags(temporary_directory)
            runner.run(input_manifest=input_manifest)

    def test_validate_input_manifest_with_required_tags_in_several_datasets(self):
        """Test that required tags for different datasets' file tags templates are validated separately and correctly
        for each dataset.
        """
        twine_with_input_manifest_with_required_tags_for_multiple_datasets = {
            "input_manifest": {
                "datasets": {
                    "first_dataset": {
                        "purpose": "A dataset containing meteorological mast data",
                        "file_tags_template": {
                            "type": "object",
                            "properties": {"manufacturer": {"type": "string"}, "height": {"type": "number"}},
                        },
                    },
                    "second_dataset": {
                        "file_tags_template": {
                            "type": "object",
                            "properties": {"is_recycled": {"type": "boolean"}, "number_of_blades": {"type": "number"}},
                        }
                    },
                }
            },
            "output_values_schema": {},
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            dataset_paths = (
                os.path.join(temporary_directory, "first_dataset"),
                os.path.join(temporary_directory, "second_dataset"),
            )

            input_manifest = {
                "id": "8ead7669-8162-4f64-8cd5-4abe92509e17",
                "datasets": {
                    "first_dataset": dataset_paths[0],
                    "second_dataset": dataset_paths[1],
                },
            }

            with Datafile(
                path=os.path.join(dataset_paths[0], "file_0.csv"),
                tags={"manufacturer": "vestas", "height": 503.7},
                mode="w",
            ) as (datafile, f):
                f.write("hello")

            with Datafile(
                path=os.path.join(dataset_paths[1], "file_1.csv"),
                tags={"is_recycled": True, "number_of_blades": 3},
                mode="w",
            ) as (datafile, f):
                f.write("hello")

            runner = Runner(app_src=app, twine=twine_with_input_manifest_with_required_tags_for_multiple_datasets)
            runner.run(input_manifest=input_manifest)

    def _make_serialised_input_manifest_with_correct_dataset_file_tags(self, dataset_path):
        """Make a serialised input manifest and create one dataset and its metadata on the filesystem so that, when
        the manifest is loaded, the dataset and its metadata are also loaded. The tags on the dataset's files are
        correct for the `TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE` twine (see the test class variable).

        :param str dataset_path: the path to make the dataset at
        :return str: the serialised input manifest
        """
        input_manifest = {"id": "8ead7669-8162-4f64-8cd5-4abe92509e17", "datasets": {"met_mast_data": dataset_path}}

        # Make two datafiles with the correct tags for `TWINE_WITH_INPUT_MANIFEST_STRAND_WITH_TAG_TEMPLATE`
        for filename in ("file_1.csv", "file_2.csv"):
            with Datafile(
                path=os.path.join(dataset_path, filename),
                tags={
                    "manufacturer": "vestas",
                    "height": 500,
                    "is_recycled": True,
                    "number_of_blades": 3,
                },
                mode="w",
            ) as (datafile, f):
                f.write("hello")

        return input_manifest


class TestRunnerDiagnostics(BaseTestCase):
    def _generate_manifests(self, serialise=False):
        """Generate configuration and input manifests containing dummy data.

        :param bool serialise: if this is `True`, serialise the manifests and use the dataset serialisation within them instead of dataset paths
        :return tuple(octue.resources.manifest.Manifest, octue.resources.manifest.Manifest): the input and configuration manifests
        """
        manifests = {}

        for data_type in ("configuration", "input"):
            dataset_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_datasets", f"{data_type}_dataset")
            datafile = Datafile(storage.path.join(dataset_path, "my_file.txt"), tags={"some": f"{data_type}_info"})

            with datafile.open("w") as f:
                f.write(f"{data_type} manifest data")

            dataset = Dataset(dataset_path, labels={f"some-{data_type}-metadata"})
            dataset.update_cloud_metadata()

            if serialise:
                dataset._instantiated_from_files_argument = True
                manifests[data_type] = Manifest(datasets={"met_mast_data": dataset}).serialise()
            else:
                manifests[data_type] = {"id": str(uuid.uuid4()), "datasets": {"met_mast_data": dataset_path}}

        return manifests["configuration"], manifests["input"]

    def test_diagnostics_with_unserialised_and_serialised_data(self):
        """Test that unserialised and serialised analysis configurations and inputs are saved to the diagnostics
        cloud path if the app crashes when the runner has been allowed to save them.
        """
        diagnostics_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "diagnostics")

        def app(analysis):
            # Mutate the configuration and inputs so we can test the originals are preserved for diagnostics.
            analysis.configuration_values = None
            analysis.configuration_manifest = None
            analysis.input_values = None
            analysis.input_manifest = None

            analysis.children["my-child"].ask(input_values=[1, 2, 3, 4])
            analysis.children["another-child"].ask(input_values="miaow")
            raise ValueError("This is deliberately raised to simulate app failure.")

        for serialise, values in [[False, {"hello": "world"}], [True, '{"hello": "world"}']]:
            with self.subTest(serialise=serialise, values=values):
                configuration_manifest, input_manifest = self._generate_manifests(serialise=serialise)

                runner = Runner(
                    app_src=app,
                    twine={
                        "configuration_values_schema": {"properties": {}},
                        "configuration_manifest": {"datasets": {}},
                        "children": [
                            {"key": "my-child"},
                            {"key": "another-child"},
                        ],
                        "input_values_schema": {},
                        "input_manifest": {"datasets": {}},
                    },
                    configuration_values=values,
                    configuration_manifest=configuration_manifest,
                    children=[
                        {
                            "key": "my-child",
                            "id": f"octue/a-child:{MOCK_SERVICE_REVISION_TAG}",
                            "backend": {
                                "name": "GCPPubSubBackend",
                                "project_id": "my-project",
                            },
                        },
                        {
                            "key": "another-child",
                            "id": f"octue/another-child:{MOCK_SERVICE_REVISION_TAG}",
                            "backend": {
                                "name": "GCPPubSubBackend",
                                "project_id": "my-project",
                            },
                        },
                    ],
                    diagnostics_cloud_path=diagnostics_cloud_path,
                    service_id="octue/my-app:2.5.7",
                )

                emulated_children = [
                    ChildEmulator(
                        events=[
                            {
                                "event": {"kind": "result", "output_values": [1, 4, 9, 16]},
                                "attributes": ATTRIBUTES,
                            }
                        ],
                    ),
                    ChildEmulator(
                        events=[
                            {"event": {"kind": "result", "output_values": "woof"}, "attributes": ATTRIBUTES},
                        ],
                    ),
                ]

                analysis_id = "4b91e3f0-4492-49e3-8061-34f1942dc68a"

                # Run the app.
                with patch("octue.runner.Child", side_effect=emulated_children):
                    with self.assertRaises(ValueError):
                        runner.run(
                            analysis_id=analysis_id,
                            input_values=values,
                            input_manifest=input_manifest,
                            save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",
                        )

                storage_client = GoogleCloudStorageClient()
                question_diagnostics_path = storage.path.join(diagnostics_cloud_path, analysis_id)

                if isinstance(values, str):
                    expected_values = values
                else:
                    expected_values = json.dumps(values)

                # Check the configuration values.
                self.assertEqual(
                    storage_client.download_as_string(
                        storage.path.join(question_diagnostics_path, "configuration_values.json")
                    ),
                    expected_values,
                )

                # Check the input values.
                self.assertEqual(
                    storage_client.download_as_string(
                        storage.path.join(question_diagnostics_path, "input_values.json")
                    ),
                    expected_values,
                )

                # Check the configuration manifest and dataset.
                configuration_manifest = Manifest.from_cloud(
                    storage.path.join(question_diagnostics_path, "configuration_manifest.json")
                )
                configuration_dataset = configuration_manifest.datasets["met_mast_data"]
                self.assertEqual(configuration_dataset.labels, {"some-configuration-metadata"})

                configuration_file = configuration_dataset.files.one()
                self.assertEqual(configuration_file.tags, {"some": "configuration_info"})

                with configuration_file.open() as f:
                    self.assertEqual(f.read(), "configuration manifest data")

                # Check the configuration dataset's path is in the diagnostics cloud directory.
                self.assertEqual(
                    configuration_dataset.path,
                    storage.path.join(question_diagnostics_path, "configuration_manifest_datasets", "met_mast_data"),
                )

                self.assertEqual(
                    configuration_file.cloud_path,
                    storage.path.join(
                        question_diagnostics_path,
                        "configuration_manifest_datasets",
                        "met_mast_data",
                        "my_file.txt",
                    ),
                )

                # Check the input manifest and dataset.
                input_manifest = Manifest.from_cloud(
                    storage.path.join(question_diagnostics_path, "input_manifest.json")
                )
                input_dataset = input_manifest.datasets["met_mast_data"]
                self.assertEqual(input_dataset.labels, {"some-input-metadata"})

                input_file = input_dataset.files.one()
                self.assertEqual(input_file.tags, {"some": "input_info"})

                with input_file.open() as f:
                    self.assertEqual(f.read(), "input manifest data")

                # Check the input dataset's path is in the diagnostics cloud directory.
                self.assertEqual(
                    input_dataset.path,
                    storage.path.join(question_diagnostics_path, "input_manifest_datasets", "met_mast_data"),
                )

                self.assertEqual(
                    input_file.cloud_path,
                    storage.path.join(
                        question_diagnostics_path, "input_manifest_datasets", "met_mast_data", "my_file.txt"
                    ),
                )

                # Check that events from the children have been recorded.
                with Datafile(storage.path.join(question_diagnostics_path, "questions.json")) as (_, f):
                    questions = json.load(f)

                # First question.
                self.assertEqual(questions[0]["key"], "my-child")
                self.assertEqual(questions[0]["id"], "octue/test-service:1.0.0")
                self.assertEqual(questions[0]["input_values"], [1, 2, 3, 4])
                self.assertEqual(questions[0]["events"][0]["event"]["output_values"], [1, 4, 9, 16])

                # Second question.
                self.assertEqual(questions[1]["key"], "another-child")
                self.assertEqual(questions[1]["id"], "octue/test-service:1.0.0")
                self.assertEqual(questions[1]["input_values"], "miaow")
                self.assertEqual(questions[1]["events"][0]["event"]["output_values"], "woof")
