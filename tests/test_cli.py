import json
import logging
import os
import tempfile
import unittest.mock
from unittest import mock
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from octue.cli import octue_cli
from octue.cloud import storage
from octue.cloud.emulators._pub_sub import MockService, MockTopic
from octue.cloud.emulators.child import ServicePatcher
from octue.configuration import AppConfiguration, ServiceConfiguration
from octue.resources import Dataset
from octue.utils.patches import MultiPatcher
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME, TESTS_DIR
from tests.base import BaseTestCase
from tests.mocks import MockOpen


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


class TestRunCommand(BaseTestCase):
    MOCK_CONFIGURATIONS = (
        ServiceConfiguration(
            name="test-app",
            namespace="testing",
            app_source_path=os.path.join(TESTS_DIR, "test_app_modules", "app_module"),
            twine_path=TWINE_FILE_PATH,
            app_configuration_path="blah.json",
        ),
        AppConfiguration(configuration_values={"n_iterations": 5}),
    )

    def test_run(self):
        """Test that the `run` CLI command runs the given service and outputs the output values."""
        with mock.patch("octue.cli.load_service_and_app_configuration", return_value=self.MOCK_CONFIGURATIONS):
            result = CliRunner().invoke(
                octue_cli,
                [
                    "run",
                    f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                ],
            )

        self.assertIn(json.dumps({"width": 3}), result.output)

    def test_run_with_output_values_file(self):
        """Test that the `run` CLI command runs the given service and stores the output values in a file if the `-o`
        option is given.
        """
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with mock.patch("octue.cli.load_service_and_app_configuration", return_value=self.MOCK_CONFIGURATIONS):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "run",
                        f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                        "-o",
                        temporary_file.name,
                    ],
                )

            with open(temporary_file.name) as f:
                self.assertEqual(json.load(f), {"width": 3})

        self.assertIn(json.dumps({"width": 3}), result.output)

    def test_run_with_output_manifest(self):
        """Test that the `run` CLI command runs the given service and stores the output manifest in a file."""
        mock_configurations = (
            ServiceConfiguration(
                name="test-app",
                namespace="testing",
                app_source_path=os.path.join(TESTS_DIR, "test_app_modules", "app_module_with_output_manifest"),
                twine_path={"input_values_schema": {}, "output_manifest": {"datasets": {}}, "output_values_schema": {}},
            ),
            AppConfiguration(),
        )

        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with mock.patch("octue.cli.load_service_and_app_configuration", return_value=mock_configurations):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "run",
                        f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                        f"--output-manifest-file={temporary_file.name}",
                    ],
                )

            with open(temporary_file.name) as f:
                self.assertIn("datasets", json.load(f))

        self.assertIn(json.dumps({"width": 3}), result.output)

    def test_run_with_monitor_messages_sent_to_file(self):
        """Test that, when the `--monitor-messages-file` is provided, any monitor messages are written to it."""
        mock_configurations = (
            ServiceConfiguration(
                name="test-app",
                namespace="testing",
                app_source_path=os.path.join(TESTS_DIR, "test_app_modules", "app_with_monitor_message"),
                twine_path=TWINE_FILE_PATH,
                app_configuration_path="blah.json",
            ),
            AppConfiguration(configuration_values={"n_iterations": 5}),
        )

        with tempfile.NamedTemporaryFile(delete=False) as monitor_messages_file:
            with mock.patch("octue.cli.load_service_and_app_configuration", return_value=mock_configurations):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "run",
                        f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                        f"--monitor-messages-file={monitor_messages_file.name}",
                    ],
                )

            with open(monitor_messages_file.name) as f:
                self.assertEqual(json.load(f), [{"status": "hello"}])

        self.assertIn(json.dumps({"width": 3}), result.output)

    def test_remote_logger_uri_can_be_set(self):
        """Test that remote logger URI can be set via the CLI and that this is logged locally."""
        with mock.patch("octue.cli.load_service_and_app_configuration", return_value=self.MOCK_CONFIGURATIONS):
            with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                CliRunner().invoke(
                    octue_cli,
                    [
                        "--logger-uri=wss://0.0.0.1:3000",
                        "run",
                        f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                    ],
                )

        mock_local_logger_emit.assert_called()


class TestStartCommand(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.python_fractal_service_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "octue",
            "templates",
            "template-fractal",
        )

        class MockOpenForConfigurationFiles(MockOpen):
            path_to_contents_mapping = {
                "octue.yaml": yaml.dump(
                    {
                        "services": [
                            {
                                "name": "test-service",
                                "namespace": "testing",
                                "app_source_path": cls.python_fractal_service_path,
                                "twine_path": os.path.join(cls.python_fractal_service_path, "twine.json"),
                                "app_configuration_path": "app_configuration.json",
                            }
                        ]
                    }
                ),
                "app_configuration.json": json.dumps(
                    {
                        "configuration_values": {
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
                        }
                    }
                ),
            }

        cls.MockOpenForConfigurationFiles = MockOpenForConfigurationFiles

    def test_start_command(self):
        """Test that the start command works without error and uses the revision tag supplied in the
        `OCTUE_SERVICE_REVISION_TAG` environment variable.
        """
        with MultiPatcher(
            patches=[
                mock.patch(
                    "octue.configuration.open",
                    unittest.mock.mock_open(mock=self.MockOpenForConfigurationFiles),
                ),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs(level=logging.INFO) as logging_context:
                    result = CliRunner().invoke(octue_cli, ["start", "--timeout=0"])

        self.assertEqual(logging_context.records[3].message, "Starting <MockService('testing/test-service:goodbye')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_command_with_revision_tag_override_when_revision_tag_environment_variable_specified(self):
        """Test that the `OCTUE_SERVICE_REVISION_TAG` is overridden by the `--revision-tag` CLI option and that a
        warning is logged when this happens.
        """
        with MultiPatcher(
            patches=[
                mock.patch(
                    "octue.configuration.open",
                    unittest.mock.mock_open(mock=self.MockOpenForConfigurationFiles),
                ),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs(level=logging.WARNING) as logging_context:
                    result = CliRunner().invoke(octue_cli, ["start", "--revision-tag=hello", "--timeout=0"])

        self.assertEqual(
            logging_context.records[3].message,
            "The `OCTUE_SERVICE_REVISION_TAG` environment variable 'goodbye' has been overridden by the "
            "`--revision-tag` CLI option 'hello'.",
        )

        self.assertEqual(logging_context.records[4].message, "Starting <MockService('testing/test-service:hello')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)


class TestGetCrashDiagnosticsCommand(BaseTestCase):
    CRASH_DIAGNOSTICS_CLOUD_PATH = storage.path.generate_gs_path(TEST_BUCKET_NAME, "crash_diagnostics")
    ANALYSIS_ID = "dc1f09ca-7037-484f-a394-8bd04866f924"

    @classmethod
    def setUpClass(cls):
        """Upload the test crash diagnostics data to the cloud storage emulator so the `octue get-crash-diagnostics`
        CLI command can be tested.

        :return None:
        """
        super().setUpClass()

        crash_diagnostics = Dataset(
            path=os.path.join(TESTS_DIR, "data", "crash_diagnostics"),
            recursive=True,
            include_octue_metadata_files=True,
        )

        crash_diagnostics.upload(storage.path.join(cls.CRASH_DIAGNOSTICS_CLOUD_PATH, cls.ANALYSIS_ID))

    def test_get_crash_diagnostics(self):
        """Test that only the values files, manifests, and questions file are downloaded when using the
        `get-crash-diagnostics` CLI command.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "get-crash-diagnostics",
                    storage.path.join(self.CRASH_DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
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
                questions[0]["messages"],
                [
                    {"type": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"type": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"type": "monitor_message", "data": '{"sample": "data"}'},
                    {"type": "result", "output_values": [1, 2, 3, 4, 5], "output_manifest": None},
                ],
            )

    def test_get_crash_diagnostics_with_datasets(self):
        """Test that datasets are downloaded as well as the values files, manifests, and questions file when the
        `get-crash-diagnostics` CLI command is run with the `--download-datasets` flag.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "get-crash-diagnostics",
                    storage.path.join(self.CRASH_DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
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
                questions[0]["messages"],
                [
                    {"type": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"type": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"type": "monitor_message", "data": '{"sample": "data"}'},
                    {"type": "result", "output_values": [1, 2, 3, 4, 5], "output_manifest": None},
                ],
            )


class TestDeployCommand(BaseTestCase):
    def test_deploy_command_group(self):
        """Test that the `dataflow` command is a subcommand of the `deploy` command."""
        result = CliRunner().invoke(octue_cli, ["deploy", "--help"])
        self.assertIn("dataflow ", result.output)

    def test_deploy_dataflow_fails_if_apache_beam_not_available(self):
        """Test that an `ImportWarning` is raised if the `dataflow deploy` CLI command is used when `apache_beam` is
        not available.
        """
        with mock.patch("importlib.util.find_spec", return_value=None):
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "deploy",
                        "dataflow",
                        f"--service-config={temporary_file.name}",
                    ],
                )

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, ImportWarning)

    def test_create_push_subscription(self):
        """Test that a push subscription can be created using the `octue deploy create-push-subscription` command and
        that its expiry time is correct.
        """
        for expiration_time_option, expected_expiration_time in (
            ([], None),
            (["--expiration-time="], None),
            (["--expiration-time=100"], 100),
        ):
            with self.subTest(expiration_time_option=expiration_time_option):
                with patch("octue.cli.Topic", new=MockTopic):
                    with patch("octue.cli.Subscription") as mock_subscription:
                        result = CliRunner().invoke(
                            octue_cli,
                            [
                                "deploy",
                                "create-push-subscription",
                                "my-project",
                                "octue",
                                "example-service",
                                "https://example.com/endpoint",
                                *expiration_time_option,
                                "--revision-tag=3.5.0",
                            ],
                        )

                    self.assertIsNone(result.exception)
                    self.assertEqual(result.exit_code, 0)

                    self.assertEqual(mock_subscription.call_args.kwargs["name"], "octue.example-service.3-5-0")
                    self.assertEqual(
                        mock_subscription.call_args.kwargs["push_endpoint"],
                        "https://example.com/endpoint",
                    )
                    self.assertEqual(mock_subscription.call_args.kwargs["expiration_time"], expected_expiration_time)
