import os
import subprocess
import time
import unittest
from unittest.mock import Mock, call

from octue.utils.processes import run_subprocess_and_log_stdout_and_stderr
from tests.base import BaseTestCase


@unittest.skipIf(condition=os.name == "nt", reason="See issue https://github.com/octue/octue-sdk-python/issues/229")
class TestRunSubprocessAndLogStdoutAndStderr(BaseTestCase):
    def test_error_raised_if_process_fails(self):
        """Test that an error is raised if the process fails."""
        mock_logger = Mock()

        with self.assertRaises(subprocess.CalledProcessError):
            run_subprocess_and_log_stdout_and_stderr(command=["blah blah blah"], logger=mock_logger, shell=True)

        log_message = mock_logger.info.call_args.args[0]
        self.assertTrue("not found" in log_message)

    def test_stdout_is_logged(self):
        """Test that any output to stdout is logged."""
        mock_logger = Mock()

        process = run_subprocess_and_log_stdout_and_stderr(
            command=["echo hello && echo goodbye"], logger=mock_logger, shell=True
        )

        self.assertEqual(process.returncode, 0)
        time.sleep(1)
        mock_logger.info.assert_has_calls([call("hello"), call("goodbye")])
