import subprocess
from unittest.mock import Mock, patch

from octue.utils.processes import run_subprocess_and_log_stdout_and_stderr
from tests.base import BaseTestCase


class TestRunSubprocessAndLogStdoutAndStderr(BaseTestCase):
    def test_error_raised_if_process_fails(self):
        """Test that an error is raised if the subprocess fails."""
        mock_logger = Mock()

        with self.assertRaises(subprocess.CalledProcessError):
            with patch(
                "octue.utils.processes.Popen",
                return_value=MockPopen(stdout_messages=[b"bash: blah: command not found"], return_code=1),
            ):
                run_subprocess_and_log_stdout_and_stderr(command=["blah"], logger=mock_logger, shell=True)

        self.assertEqual(mock_logger.info.call_args[0][0], "bash: blah: command not found")

    def test_stdout_is_logged(self):
        """Test that any output to stdout from a subprocess is logged."""
        mock_logger = Mock()

        with patch(
            "octue.utils.processes.Popen", return_value=MockPopen(stdout_messages=[b"hello", b"goodbye"], return_code=0)
        ):
            process = run_subprocess_and_log_stdout_and_stderr(
                command=["echo hello && echo goodbye"], logger=mock_logger, shell=True
            )

        self.assertEqual(process.returncode, 0)
        self.assertEqual(mock_logger.info.call_args_list[0][0][0], "hello")
        self.assertEqual(mock_logger.info.call_args_list[1][0][0], "goodbye")


class MockPopen:
    """A mock of subprocess.Popen.

    :param iter(bytes) stdout_messages:
    :param int return_code:
    :return None:
    """

    def __init__(self, stdout_messages, return_code=0, *args, **kwargs):
        self.stdout = MockBufferedReader(stdout_messages)
        self.returncode = return_code

    def wait(self):
        """Do nothing.

        :return None:
        """
        pass


class MockBufferedReader:
    """A mock io.BufferedReader.

    :param iter(bytes) contents:
    :return None:
    """

    def __init__(self, contents):
        self.contents = contents
        self._line_count = 0

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def readline(self):
        """Read a line from the mocked buffered reader.

        :return str:
        """
        try:
            line = self.contents[self._line_count]
        except IndexError:
            raise StopIteration()

        self._line_count += 1
        return line
