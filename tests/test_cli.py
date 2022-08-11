import json
import os
import tempfile
import unittest.mock
import uuid
from unittest import mock

import yaml
from click.testing import CliRunner

from octue import Runner
from octue.cli import octue_cli
from octue.cloud import storage
from octue.cloud.emulators._pub_sub import MockService
from octue.cloud.emulators.child import ServicePatcher
from octue.configuration import AppConfiguration, ServiceConfiguration
from octue.resources import Datafile
from tests import TEST_BUCKET_NAME, TESTS_DIR
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
    def test_start_command(self):
        """Test that the start command works without error."""
        python_fractal_service_path = os.path.join(
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
                                "app_source_path": python_fractal_service_path,
                                "twine_path": os.path.join(python_fractal_service_path, "twine.json"),
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
                                "project_name": "octue-amy",
                            },
                        }
                    }
                ),
            }

        with mock.patch("octue.configuration.open", unittest.mock.mock_open(mock=MockOpenForConfigurationFiles)):
            with ServicePatcher():
                with mock.patch("octue.cli.Service", MockService):
                    result = CliRunner().invoke(octue_cli, ["start", "--timeout=0"])

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)


class TestGetCrashDiagnosticsCommand(BaseTestCase):
    def test_get_crash_diagnostics(self):
        """Test the get crash diagnostics CLI command."""
        crash_diagnostics_cloud_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "crash_diagnostics")

        def app(analysis):
            raise ValueError("This is deliberately raised to simulate app failure.")

        manifests = {}

        for data_type in ("configuration", "input"):
            dataset_path = storage.path.generate_gs_path(TEST_BUCKET_NAME, "my_datasets", f"{data_type}_dataset")

            with Datafile(storage.path.join(dataset_path, "my_file.txt"), mode="w") as (datafile, f):
                f.write(f"{data_type} manifest data")

            manifests[data_type] = {"id": str(uuid.uuid4()), "datasets": {"met_mast_data": dataset_path}}

        runner = Runner(
            app_src=app,
            twine={
                "configuration_values_schema": {"properties": {}},
                "configuration_manifest": {"datasets": {}},
                "input_values_schema": {},
                "input_manifest": {"datasets": {}},
            },
            configuration_values={"getting": "ready"},
            configuration_manifest=manifests["configuration"],
            crash_diagnostics_cloud_path=crash_diagnostics_cloud_path,
        )

        analysis_id = "4b91e3f0-4492-49e3-8061-34f1942dc68a"

        with self.assertRaises(ValueError):
            runner.run(
                analysis_id=analysis_id,
                input_values={"hello": "world"},
                input_manifest=manifests["input"],
                allow_save_diagnostics_data_on_crash=True,
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "get-crash-diagnostics",
                    storage.path.join(crash_diagnostics_cloud_path, analysis_id),
                    "--local-path",
                    temporary_directory,
                ],
            )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            directory_contents = list(os.walk(temporary_directory))
            self.assertEqual(directory_contents[0][1], [analysis_id])
            self.assertEqual(
                set(directory_contents[1][1]),
                {"configuration_manifest_datasets", "input_manifest_datasets"},
            )
            self.assertEqual(
                set(directory_contents[1][2]),
                {
                    "configuration_values.json",
                    "configuration_manifest.json",
                    "input_manifest.json",
                    "input_values.json",
                    "messages.json",
                },
            )
            self.assertEqual(directory_contents[2][1], ["met_mast_data"])
            self.assertEqual(set(directory_contents[3][2]), {"my_file.txt", ".octue"})
            self.assertEqual(directory_contents[4][1], ["met_mast_data"])
            self.assertEqual(set(directory_contents[5][2]), {"my_file.txt", ".octue"})


class TestDeployCommand(BaseTestCase):
    def test_deploy_command_group(self):
        """Test that the `dataflow` command is a subcommand of the `deploy` command."""
        result = CliRunner().invoke(octue_cli, ["deploy", "--help"])
        self.assertIn("cloud-run ", result.output)
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
