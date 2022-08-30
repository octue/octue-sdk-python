import logging
import subprocess
import time
from unittest import TestCase
from unittest.mock import Mock, patch

from octue.utils.threads import RepeatingTimer, run_subprocess_and_log_stdout_and_stderr


class TestRepeatingTimer(TestCase):
    def test_calls_function_expected_number_of_times_before_cancellation(self):
        """Test that the timer calls the function the expected number of times before cancellation."""
        mock_function = Mock()
        timer = RepeatingTimer(interval=0.1, function=mock_function)
        timer.daemon = True

        timer.start()
        time.sleep(1)
        timer.cancel()

        self.assertEqual(mock_function.call_count, 9)

    def test_calls_function_at_correct_intervals(self):
        """Test that the timer calls the function at the near-correct time interval."""
        intended_interval = 0.1
        times = []

        timer = RepeatingTimer(interval=intended_interval, function=lambda: times.append(time.time()))
        timer.daemon = True

        timer.start()
        time.sleep(1)
        timer.cancel()

        intervals = []

        for i, timestamp in enumerate(times):
            if i == 0:
                continue

            intervals.append(timestamp - times[i - 1])

        for interval in intervals:
            self.assertAlmostEqual(interval, intended_interval, delta=0.05)

    def test_does_not_call_function_if_cancelled_before_interval(self):
        """Test that the function is not called if the timer is cancelled before the time interval is first reached."""
        mock_function = Mock()
        timer = RepeatingTimer(interval=10, function=mock_function)
        timer.daemon = True

        timer.start()
        timer.cancel()

        mock_function.assert_not_called()


class TestRunSubprocessAndLogStdoutAndStderr(TestCase):
    def test_error_raised_if_process_fails(self):
        """Test that an error is raised if the subprocess fails."""
        with self.assertRaises(subprocess.CalledProcessError):
            with patch(
                "octue.utils.threads.Popen",
                return_value=MockPopen(stdout_messages=[b"bash: blah: command not found"], return_code=1),
            ):
                run_subprocess_and_log_stdout_and_stderr(command=["blah"], logger=logging.getLogger(), shell=True)

    def test_stdout_is_logged(self):
        """Test that any output to stdout from a subprocess is logged."""
        mock_logger = Mock()

        with patch(
            "octue.utils.threads.Popen", return_value=MockPopen(stdout_messages=[b"hello", b"goodbye"], return_code=0)
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
