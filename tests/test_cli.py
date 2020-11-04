import os

from click.testing import CliRunner

from octue.cli import octue_cli
from tests import TESTS_DIR
from tests.app import CUSTOM_APP_RUN_MESSAGE
from tests.base import BaseTestCase


class RunnerTestCase(BaseTestCase):

    TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")

    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(octue_cli, ['--version'])
        assert 'version' in result.output

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(octue_cli, ['--help'])
        assert help_result.output.startswith('Usage')

        h_result = CliRunner().invoke(octue_cli, ['-h'])
        assert help_result.output == h_result.output

    def test_run_command_can_be_added(self):
        """Test that an arbitrary run command can be used in the run command of the CLI."""
        result = CliRunner().invoke(
            octue_cli,
            [
                'run',
                f'--app-dir={TESTS_DIR}',
                f'--twine={self.TWINE_FILE_PATH}',
                f'--config-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "configuration")}',
                f'--input-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests", "input")}'
            ]
        )

        assert CUSTOM_APP_RUN_MESSAGE in result.output

    def test_run_command_works_with_data_dir(self):
        """Test that the run command of the CLI works with the --data-dir option."""
        result = CliRunner().invoke(
            octue_cli,
            [
                'run',
                f'--app-dir={TESTS_DIR}',
                f'--twine={self.TWINE_FILE_PATH}',
                f'--data-dir={os.path.join(TESTS_DIR, "data", "data_dir_with_no_manifests")}'
            ]
        )

        assert CUSTOM_APP_RUN_MESSAGE in result.output
