import logging
import os
import subprocess
import unittest

from octue.utils.processes import run_subprocess_and_log_stdout_and_stderr
from tests.base import BaseTestCase


@unittest.skipIf(condition=os.name == "nt", reason="See issue https://github.com/octue/octue-sdk-python/issues/229")
class TestRunSubprocessAndLogStdoutAndStderr(BaseTestCase):
    def test_error_raised_if_process_fails(self):
        """Test that an error is raised if the process fails."""
        with self.assertLogs() as logs_context_manager:
            with self.assertRaises(subprocess.CalledProcessError):
                run_subprocess_and_log_stdout_and_stderr(
                    command=["blah blah blah"], logger=logging.getLogger(), shell=True
                )

            self.assertIn("not found", logs_context_manager.records[0].message)

    def test_stdout_is_logged(self):
        """Test that any output to stdout is logged."""
        with self.assertLogs() as logs_context_manager:
            process = run_subprocess_and_log_stdout_and_stderr(
                command=["echo hello && echo goodbye"], logger=logging.getLogger(), shell=True
            )

            self.assertEqual(process.returncode, 0)
            self.assertEqual(logs_context_manager.records[0].message, "hello")
            self.assertEqual(logs_context_manager.records[1].message, "goodbye")
