from click.testing import CliRunner

from octue.cli import octue_cli
from .base import BaseTestCase


class RunnerTestCase(BaseTestCase):

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
