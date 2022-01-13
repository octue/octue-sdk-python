import os
import tempfile
import uuid
from unittest import mock
from click.testing import CliRunner

from octue import REPOSITORY_ROOT
from octue.cli import octue_cli
from octue.cloud.deployment.google.dataflow.deploy import DATAFLOW_TEMPORARY_FILES_LOCATION
from tests import TESTS_DIR
from tests.base import BaseTestCase
from tests.test_app_modules.app_module.app import CUSTOM_APP_RUN_MESSAGE


TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")


class TestCLI(BaseTestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(octue_cli, ["--version"])
        assert "version" in result.output

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(octue_cli, ["--help"])
        assert help_result.output.startswith("Usage")

        h_result = CliRunner().invoke(octue_cli, ["-h"])
        assert help_result.output == h_result.output


class TestRunCommand(BaseTestCase):
    def test_run(self):
        """Test that an arbitrary run command can be used in the run command of the CLI."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "run",
                    f"--app-dir={os.path.join(TESTS_DIR, 'test_app_modules', 'app_module')}",
                    f"--twine={TWINE_FILE_PATH}",
                    f'--config-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "configuration")}',
                    f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                    f"--output-dir={temporary_directory}",
                ],
            )

        assert CUSTOM_APP_RUN_MESSAGE in result.output

    def test_run_with_data_dir(self):
        """Test that the run command of the CLI works with the --data-dir option."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "run",
                    f"--app-dir={os.path.join(TESTS_DIR, 'test_app_modules', 'app_module')}",
                    f"--twine={TWINE_FILE_PATH}",
                    f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                    f"--output-dir={temporary_directory}",
                ],
            )

        assert CUSTOM_APP_RUN_MESSAGE in result.output

    def test_remote_logger_uri_can_be_set(self):
        """Test that remote logger URI can be set via the CLI and that this is logged locally."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                CliRunner().invoke(
                    octue_cli,
                    [
                        "--logger-uri=wss://0.0.0.1:3000",
                        "run",
                        f"--app-dir={TESTS_DIR}",
                        f"--twine={TWINE_FILE_PATH}",
                        f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                        f"--output-dir={temporary_directory}",
                    ],
                )

            mock_local_logger_emit.assert_called()


class TestStartCommand(BaseTestCase):
    def test_start_command(self):
        """Test that the start command works without error."""
        elevation_service_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "octue",
            "templates",
            "template-child-services",
            "elevation_service",
        )

        result = CliRunner().invoke(
            octue_cli,
            [
                "start",
                f"--app-dir={elevation_service_path}",
                f"--twine={os.path.join(elevation_service_path, 'twine.json')}",
                f"--config-dir={os.path.join(elevation_service_path, 'data', 'configuration')}",
                f"--service-id={uuid.uuid4()}",
                "--timeout=0",
            ],
        )

        self.assertEqual(result.exit_code, 0)


class TestDeployCommand(BaseTestCase):
    def test_deploy_command_group(self):
        """Test that the `dataflow` command is a subcommand of the `deploy` command."""
        result = CliRunner().invoke(octue_cli, ["deploy", "--help"])
        self.assertIn("cloud-run ", result.output)
        self.assertIn("dataflow ", result.output)

    def test_deploy_dataflow(self):
        """Test that the `octue deploy dataflow` command works."""
        with mock.patch("octue.cloud.deployment.google.dataflow.deploy.apache_beam.Pipeline") as mock_pipeline:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "deploy",
                    "dataflow",
                    "my-service",
                    "octue.services.1df81225-7a87-4b1c-9313-cdc376a127a7",
                    "my-project",
                    "my-region",
                    "--image-uri=a.uri/image-name:latest",
                    "--runner=MyApacheBeamRunner",
                ],
            )

        pipeline_options = mock_pipeline.call_args.kwargs["options"].get_all_options()

        self.assertEqual(pipeline_options["project"], "my-project")
        self.assertEqual(pipeline_options["region"], "my-region")
        self.assertEqual(pipeline_options["temp_location"], DATAFLOW_TEMPORARY_FILES_LOCATION)
        self.assertEqual(pipeline_options["job_name"], "my-service")
        self.assertEqual(pipeline_options["runner"], "MyApacheBeamRunner")
        self.assertEqual(pipeline_options["dataflow_service_options"], ["enable_prime"])
        self.assertEqual(pipeline_options["setup_file"], os.path.join(REPOSITORY_ROOT, "setup.py"))
        self.assertEqual(pipeline_options["sdk_container_image"], "a.uri/image-name:latest")

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)
