import google.api_core
from google.api_core import retry

from .subscription import Subscription
from .topic import Topic


__all__ = ["Subscription", "Topic", "create_custom_retry"]


def create_custom_retry(timeout):
    """Create a custom `Retry` object specifying that the given Google Cloud request should retry for the given amount
    of time for the given exceptions.

    :param float timeout:
    :return google.api_core.retry.Retry:
    """
    return retry.Retry(
        maximum=timeout / 4,
        deadline=timeout,
        predicate=google.api_core.retry.if_exception_type(
            google.api_core.exceptions.NotFound,
            google.api_core.exceptions.Aborted,
            google.api_core.exceptions.DeadlineExceeded,
            google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ResourceExhausted,
            google.api_core.exceptions.ServiceUnavailable,
            google.api_core.exceptions.Unknown,
            google.api_core.exceptions.Cancelled,
        ),
    )
