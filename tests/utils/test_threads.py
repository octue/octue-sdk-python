import time
from unittest import TestCase
from unittest.mock import Mock

from octue.utils.threads import RepeatingTimer


class TestRepeatingTimer(TestCase):
    def test_calls_function_expected_number_of_times_before_cancellation(self):
        """Test that the timer calls the function the expected number of times before cancellation."""
        mock_function = Mock()
        timer = RepeatingTimer(interval=0.1, function=mock_function)
        timer.daemon = True

        timer.start()
        time.sleep(1)
        timer.cancel()

        self.assertGreaterEqual(mock_function.call_count, 4)

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
            self.assertAlmostEqual(interval, intended_interval, delta=0.2)

    def test_does_not_call_function_if_cancelled_before_interval(self):
        """Test that the function is not called if the timer is cancelled before the time interval is first reached."""
        mock_function = Mock()
        timer = RepeatingTimer(interval=10, function=mock_function)
        timer.daemon = True

        timer.start()
        timer.cancel()

        mock_function.assert_not_called()
