import logging
import subprocess
from unittest.mock import patch

from octue.utils.processes import run_subprocess_and_log_stdout_and_stderr
from tests.base import BaseTestCase


LOGGER = logging.getLogger(__name__)


class TestRunSubprocessAndLogStdoutAndStderr(BaseTestCase):
    def test_error_raised_if_process_fails(self):
        """Test that an error is raised if the process fails."""
        with patch("logging.StreamHandler.emit") as mock_local_logger_emit:

            with self.assertRaises(subprocess.CalledProcessError):
                run_subprocess_and_log_stdout_and_stderr(command=["blah blah blah"], logger=LOGGER, shell=True)

        self.assertIn("blah: command not found", mock_local_logger_emit.call_args_list[0][0][0].msg)

    def test_stdout_is_logged(self):
        """Test that any output to stdout is logged."""
        with patch("logging.StreamHandler.emit") as mock_local_logger_emit:
            process = run_subprocess_and_log_stdout_and_stderr(
                command=["echo hello && echo goodbye"], logger=LOGGER, shell=True
            )

        self.assertEqual(process.returncode, 0)
        self.assertEqual(mock_local_logger_emit.call_args_list[0][0][0].msg, "hello")
        self.assertEqual(mock_local_logger_emit.call_args_list[1][0][0].msg, "goodbye")
