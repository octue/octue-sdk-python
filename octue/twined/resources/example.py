import functools
import logging
import uuid

logger = logging.getLogger(__name__)


def run_fibonacci_example(n):
    """Run an example analysis that calculates the first `n` values of the Fibonacci sequence.

    :param int n: the number of values in the Fibonacci sequence to calculate the value of (must be more than 0)
    :return (dict, str): the result event and a random question UUID
    """
    error_message = f"`n` must be an integer >= 0. Received {n!r}."

    if not isinstance(n, int):
        raise TypeError(error_message)

    if n < 0:
        raise ValueError(error_message)

    logger.info("Starting Fibonacci sequence calculation.")

    answer = {
        "kind": "result",
        "output_values": {"fibonacci": [_calculate_fibonacci(i) for i in range(n)]},
        "output_manifest": None,
    }

    logger.info("Finished Fibonacci sequence calculation.")
    return answer, str(uuid.uuid4())


@functools.lru_cache()
def _calculate_fibonacci(n):
    """Calculate the nth value of the Fibonacci sequence.

    :param int n: the position in the sequence to calculate the value of (must be more than 0)
    :return int: the value of the sequence at the nth position
    """
    if n == 0:
        return 0

    if n == 1:
        return 1

    return _calculate_fibonacci(n - 1) + _calculate_fibonacci(n - 2)
