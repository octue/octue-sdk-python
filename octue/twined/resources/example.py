import functools
import logging

logger = logging.getLogger(__name__)


def calculate_fibonacci_sequence(n):
    """Run an example analysis that calculates the first `n` values of the Fibonacci sequence.

    :param int n: the number of values in the Fibonacci sequence to calculate (must be >= 0)
    :return list(int): the sequence
    """
    error_message = f"`n` must be an integer >= 0. Received {n!r}."

    if not isinstance(n, int):
        raise TypeError(error_message)

    if n < 0:
        raise ValueError(error_message)

    logger.info("Starting Fibonacci sequence calculation.")
    sequence = [_calculate_fibonacci_value(i) for i in range(n)]
    logger.info("Finished Fibonacci sequence calculation.")
    return sequence


@functools.lru_cache()
def _calculate_fibonacci_value(n):
    """Calculate the nth value of the Fibonacci sequence.

    :param int n: the position in the sequence to calculate the value of (must be >= 0)
    :return int: the value of the sequence at the nth position
    """
    if n == 0:
        return 0

    if n == 1:
        return 1

    return _calculate_fibonacci_value(n - 1) + _calculate_fibonacci_value(n - 2)
