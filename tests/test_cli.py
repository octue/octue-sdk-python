import json
import logging
import os
import tempfile
from unittest import mock
from unittest.mock import patch

from click.testing import CliRunner

from octue.cli import octue_cli
from octue.cloud import storage
from octue.cloud.emulators._pub_sub import MockService
from octue.cloud.emulators.service import ServicePatcher
from octue.configuration import AppConfiguration, ServiceConfiguration
from octue.resources import Dataset
from octue.utils.patches import MultiPatcher
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME, TESTS_DIR
from tests.base import BaseTestCase

TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")


class TestCLI(BaseTestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(octue_cli, ["--version"])
        self.assertIn("version", result.output)

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(octue_cli, ["--help"])
        self.assertTrue(help_result.output.startswith("Usage"))

        h_result = CliRunner().invoke(octue_cli, ["-h"])
        self.assertEqual(help_result.output, h_result.output)


class TestQuestionAskRemoteCommand(BaseTestCase):
    MOCK_CONFIGURATIONS = (
        ServiceConfiguration(name="test-app", namespace="testing"),
        AppConfiguration(configuration_values={"n_iterations": 5}),
    )

    SRUID = "my-org/my-service:1.0.0"
    QUESTION_UUID = "81f35b28-068b-4314-9eeb-e55e60d0fe8a"

    def test_with_input_values(self):
        """Test that the `octue question ask remote` CLI command works with just input values."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.MOCK_CONFIGURATIONS[0]):
            with mock.patch("octue.cli.Child.ask", return_value=({"some": "data"}, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "question",
                        "ask",
                        "remote",
                        self.SRUID,
                        '--input-values={"height": 3}',
                    ],
                )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=False)
        self.assertIn(json.dumps({"some": "data"}), result.output)

    def test_with_input_manifest(self):
        """Test that the `octue question ask remote` CLI command works with just an input manifest."""
        input_manifest = self.create_valid_manifest()

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.MOCK_CONFIGURATIONS[0]):
            with mock.patch("octue.cli.Child.ask", return_value=({"some": "data"}, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "question",
                        "ask",
                        "remote",
                        self.SRUID,
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        self.assertEqual(mock_ask.call_args.kwargs["input_manifest"].id, input_manifest.id)
        self.assertIn(json.dumps({"some": "data"}), result.output)

    def test_with_input_values_and_manifest(self):
        """Test that the `octue question ask remote` CLI command works with input values and input manifest."""
        input_values = {"height": 3}
        input_manifest = self.create_valid_manifest()

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.MOCK_CONFIGURATIONS[0]):
            with mock.patch("octue.cli.Child.ask", return_value=({"some": "data"}, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "question",
                        "ask",
                        "remote",
                        self.SRUID,
                        f"--input-values={json.dumps(input_values)}",
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        self.assertEqual(mock_ask.call_args.kwargs["input_values"], input_values)
        self.assertEqual(mock_ask.call_args.kwargs["input_manifest"].id, input_manifest.id)
        self.assertIn(json.dumps({"some": "data"}), result.output)

    def test_asynchronous(self):
        """Test that the `octue question ask remote` CLI command works with the `--asynchronous` option and returns the
        question UUID.
        """
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.MOCK_CONFIGURATIONS[0]):
            with mock.patch("octue.cli.Child.ask", return_value=({"some": "data"}, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "question",
                        "ask",
                        "remote",
                        self.SRUID,
                        '--input-values={"height": 3}',
                        "--asynchronous",
                    ],
                )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=True)
        self.assertIn(self.QUESTION_UUID, result.output)

    def test_with_no_service_configuration(self):
        """Test that the command works when no service configuration is provided."""
        with mock.patch("octue.cli.Child.ask", return_value=({"some": "data"}, self.QUESTION_UUID)) as mock_ask:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "question",
                    "ask",
                    "remote",
                    self.SRUID,
                    '--input-values={"height": 3}',
                ],
            )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=False)
        self.assertIn(json.dumps({"some": "data"}), result.output)


class TestStartCommand(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.python_fractal_service_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "octue",
            "templates",
            "template-fractal",
        )

        cls.service_configuration = ServiceConfiguration(
            name="test-service",
            namespace="testing",
            app_source_path=cls.python_fractal_service_path,
            twine_path=os.path.join(cls.python_fractal_service_path, "twine.json"),
            app_configuration_path="app_configuration.json",
        )

        cls.app_configuration = AppConfiguration(
            configuration_values={
                "width": 600,
                "height": 600,
                "n_iterations": 64,
                "color_scale": "YlGnBu",
                "type": "png",
                "x_range": [-1.5, 0.6],
                "y_range": [-1.26, 1.26],
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_name": "octue-sdk-python",
                },
            },
        )

    def test_start_command(self):
        """Test that the start command works without error and uses the revision tag supplied in the
        `OCTUE_SERVICE_REVISION_TAG` environment variable.
        """
        with MultiPatcher(
            patches=[
                mock.patch(
                    "octue.cli.load_service_and_app_configuration",
                    return_value=(self.service_configuration, self.app_configuration),
                ),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs(level=logging.INFO) as logging_context:
                    result = CliRunner().invoke(octue_cli, ["start", "--timeout=0"])

        self.assertEqual(logging_context.records[1].message, "Starting <MockService('testing/test-service:goodbye')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_command_with_revision_tag_override_when_revision_tag_environment_variable_specified(self):
        """Test that the `OCTUE_SERVICE_REVISION_TAG` is overridden by the `--revision-tag` CLI option and that a
        warning is logged when this happens.
        """
        with MultiPatcher(
            patches=[
                mock.patch(
                    "octue.cli.load_service_and_app_configuration",
                    return_value=(self.service_configuration, self.app_configuration),
                ),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs() as logging_context:
                    result = CliRunner().invoke(octue_cli, ["start", "--revision-tag=hello", "--timeout=0"])

        self.assertEqual(
            logging_context.records[1].message,
            "The `OCTUE_SERVICE_REVISION_TAG` environment variable 'goodbye' has been overridden by the "
            "`--revision-tag` CLI option 'hello'.",
        )

        self.assertEqual(logging_context.records[2].message, "Starting <MockService('testing/test-service:hello')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)


class TestQuestionDiagnosticsCommand(BaseTestCase):
    DIAGNOSTICS_CLOUD_PATH = storage.path.generate_gs_path(TEST_BUCKET_NAME, "diagnostics")
    ANALYSIS_ID = "dc1f09ca-7037-484f-a394-8bd04866f924"

    @classmethod
    def setUpClass(cls):
        """Upload the test diagnostics data to the cloud storage emulator so the `octue question diagnostics` CLI command can
        be tested.

        :return None:
        """
        super().setUpClass()

        diagnostics = Dataset(
            path=os.path.join(TESTS_DIR, "data", "diagnostics"),
            recursive=True,
            include_octue_metadata_files=True,
        )

        diagnostics.upload(storage.path.join(cls.DIAGNOSTICS_CLOUD_PATH, cls.ANALYSIS_ID))

    def test_warning_logged_if_no_diagnostics_found(self):
        """Test that a warning about there being no diagnostics is logged if the diagnostics cloud path is empty."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, "9f4ccee3-15b0-4a03-b5ac-c19e1d66a709"),
                    "--local-path",
                    temporary_directory,
                ],
            )

        self.assertIn(
            "Attempted to download files from 'gs://octue-sdk-python-test-bucket/diagnostics/9f4ccee3-15b0-4a03-b5ac-"
            "c19e1d66a709' but it appears empty. Please check this is the correct path.",
            result.output,
        )

        self.assertIn(
            "No diagnostics found at 'gs://octue-sdk-python-test-bucket/diagnostics/9f4ccee3-15b0-4a03-b5ac-"
            "c19e1d66a709'",
            result.output,
        )

        self.assertNotIn("Downloaded diagnostics from", result.output)

    def test_get_diagnostics(self):
        """Test that only the values files, manifests, and questions file are downloaded when using the
        `question diagnostics` CLI command.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
                    "--local-path",
                    temporary_directory,
                ],
            )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            # Only the values files, manifests, and messages should be downloaded.
            directory_contents = list(os.walk(temporary_directory))
            self.assertEqual(len(directory_contents), 2)
            self.assertEqual(directory_contents[0][1], [self.ANALYSIS_ID])

            self.assertEqual(directory_contents[1][1], [])

            self.assertEqual(
                set(directory_contents[1][2]),
                {
                    "configuration_values.json",
                    "configuration_manifest.json",
                    "input_manifest.json",
                    "input_values.json",
                    "questions.json",
                },
            )

            # Check the questions have been downloaded.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "questions.json")) as f:
                questions = json.load(f)

            self.assertEqual(questions[0]["id"], f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}")

            self.assertEqual(
                questions[0]["events"],
                [
                    {"kind": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"kind": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"kind": "monitor_message", "data": {"sample": "data"}},
                    {"kind": "result", "output_values": [1, 2, 3, 4, 5]},
                ],
            )

    def test_get_diagnostics_with_datasets(self):
        """Test that datasets are downloaded as well as the values files, manifests, and questions file when the
        `question diagnostics` CLI command is run with the `--download-datasets` flag.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
                    "--local-path",
                    temporary_directory,
                    "--download-datasets",
                ],
            )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            # Check the configuration dataset has been downloaded.
            configuration_dataset_path = os.path.join(
                temporary_directory,
                self.ANALYSIS_ID,
                "configuration_manifest_datasets",
                "configuration_dataset",
            )

            configuration_dataset = Dataset(configuration_dataset_path)
            self.assertEqual(configuration_dataset.tags, {"some": "metadata"})
            self.assertEqual(configuration_dataset.files.one().name, "my_file.txt")

            # Check that the configuration manifest has been updated to use the local paths for its datasets.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "configuration_manifest.json")) as f:
                self.assertEqual(json.load(f)["datasets"]["configuration_dataset"], configuration_dataset_path)

            # Check the input dataset has been downloaded.
            input_dataset_path = os.path.join(
                temporary_directory,
                self.ANALYSIS_ID,
                "input_manifest_datasets",
                "input_dataset",
            )

            input_dataset = Dataset(input_dataset_path)
            self.assertEqual(input_dataset.tags, {"more": "metadata"})
            self.assertEqual(input_dataset.files.one().name, "my_file.txt")

            # Check that the input manifest has been updated to use the local paths for its datasets.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "input_manifest.json")) as f:
                self.assertEqual(json.load(f)["datasets"]["input_dataset"], input_dataset_path)

            # Check the questions have been downloaded.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "questions.json")) as f:
                questions = json.load(f)

            self.assertEqual(questions[0]["id"], f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}")

            self.assertEqual(
                questions[0]["events"],
                [
                    {"kind": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"kind": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"kind": "monitor_message", "data": {"sample": "data"}},
                    {"kind": "result", "output_values": [1, 2, 3, 4, 5]},
                ],
            )
