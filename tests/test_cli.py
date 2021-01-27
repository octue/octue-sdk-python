import os
import tempfile
from unittest import mock
from click.testing import CliRunner
from tests import TESTS_DIR
from tests.app import CUSTOM_APP_RUN_MESSAGE
from tests.base import BaseTestCase

from octue.cli import octue_cli


class RunnerTestCase(BaseTestCase):

    TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")

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

    def test_run_command_can_be_added(self):
        """Test that an arbitrary run command can be used in the run command of the CLI."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "run",
                    f"--app-dir={TESTS_DIR}",
                    f"--twine={self.TWINE_FILE_PATH}",
                    f'--config-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "configuration")}',
                    f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}',
                    f"--output-dir={temporary_directory}",
                ],
            )

        assert CUSTOM_APP_RUN_MESSAGE in result.output

    def test_run_command_works_with_data_dir(self):
        """Test that the run command of the CLI works with the --data-dir option."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "run",
                    f"--app-dir={TESTS_DIR}",
                    f"--twine={self.TWINE_FILE_PATH}",
                    f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                    f"--output-dir={temporary_directory}",
                ],
            )

        assert CUSTOM_APP_RUN_MESSAGE in result.output

    def test_package_logs_are_streamed_if_asked_for(self):
        """ Test that logs from the main part of the package are sent to stderr if the CLI option is enabled. """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("logging.StreamHandler.emit") as mock_log_handler_emit:
                CliRunner().invoke(
                    octue_cli,
                    [
                        "--show-twined-logs",
                        "run",
                        f"--app-dir={TESTS_DIR}",
                        f"--twine={self.TWINE_FILE_PATH}",
                        f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                        f"--output-dir={temporary_directory}",
                    ],
                )

            mock_log_handler_emit.assert_called()

            self.assertTrue(
                any(
                    "Showing package logs as well as analysis logs (the package logs are recommended for software "
                    "engineers but may still be useful to app development by scientists." in arg[0][0].msg
                    for arg in mock_log_handler_emit.call_args_list
                )
            )

    def test_package_logs_are_not_streamed_if_not_asked_for(self):
        """ Test that logs from the main part of the package aren't sent to stderr if the CLI option is not enabled. """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("logging.StreamHandler.emit") as mock_log_handler_emit:
                CliRunner().invoke(
                    octue_cli,
                    [
                        "run",
                        f"--app-dir={TESTS_DIR}",
                        f"--twine={self.TWINE_FILE_PATH}",
                        f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                        f"--output-dir={temporary_directory}",
                    ],
                )

            mock_log_handler_emit.assert_called()

            self.assertFalse(
                any(
                    "Showing package logs as well as analysis logs (the package logs are recommended for software "
                    "engineers but may still be useful to app development by scientists." in arg[0][0].msg
                    for arg in mock_log_handler_emit.call_args_list
                )
            )

    def test_remote_logger_uri_can_be_set(self):
        """ Test that remote logger URI can be set via the CLI and that this is logged locally. """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                CliRunner().invoke(
                    octue_cli,
                    [
                        "--logger-uri=wss://0.0.0.1:3000",
                        "run",
                        f"--app-dir={TESTS_DIR}",
                        f"--twine={self.TWINE_FILE_PATH}",
                        f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}',
                        f"--output-dir={temporary_directory}",
                    ],
                )

            mock_local_logger_emit.assert_called()

    def test_start_command(self):
        """ Test that the start command works without error. """
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
                "--service-id=8dgd07fa-6bcd-4ec3-a331-69f737a15332",
                "--timeout=5",
            ],
        )

        self.assertEqual(result.exit_code, 0)
