import functools
import logging
import uuid

logger = logging.getLogger(__name__)


def run_example(n):
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

    :param int n: the position in the sequence in the sequence to calculate the value of (must be more than 0)
    :return int: the value of the sequence at the nth position
    """
    if not isinstance(n, int):
        raise TypeError(f"`n` must be an integer. Received {n!r}.")

    if n < 0:
        raise ValueError(f"`n` must be an integer >= 0. Received {n!r}.")

    if n == 0:
        return 0

    if n == 1:
        return 1

    return _calculate_fibonacci(n - 1) + _calculate_fibonacci(n - 2)
