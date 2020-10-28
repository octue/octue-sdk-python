from octue.cli import octue_cli
from .base import BaseTestCase


class RunnerTestCase(BaseTestCase):

    def test_version(self):
        """Ensure the version command works in the CLI."""
        with self.assertRaises(SystemExit):
            octue_cli(['--version'])
