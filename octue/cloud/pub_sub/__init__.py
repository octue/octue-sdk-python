from octue.cloud.events import OCTUE_SERVICES_PREFIX
from octue.cloud.service_id import convert_service_id_to_pub_sub_form

from .subscription import Subscription
from .topic import Topic


__all__ = ["Subscription", "Topic"]


def create_push_subscription(
    project_name,
    sruid,
    push_endpoint,
    subscription_filter=None,
    expiration_time=None,
    allow_existing=True,
):
    """Create a Google Pub/Sub push subscription for an Octue service for it to receive questions from parents. If a
    corresponding topic doesn't exist, it will be created first.

    :param str project_name: the name of the Google Cloud project in which the subscription will be created
    :param str sruid: the SRUID (service revision unique identifier)
    :param str push_endpoint: the HTTP/HTTPS endpoint of the service to push to. It should be fully formed and include the 'https://' prefix
    :param str|None subscription_filter: if specified, the filter to apply to the subscription; otherwise, no filter is applied
    :param float|None expiration_time: the number of seconds of inactivity after which the subscription should expire. If not provided, no expiration time is applied to the subscription
    :param bool allow_existing: if True, don't raise an error if the subscription already exists
    :return octue.cloud.pub_sub.subscription.Subscription:
    """
    if expiration_time:
        expiration_time = float(expiration_time)
    else:
        expiration_time = None

    subscription = Subscription(
        name=convert_service_id_to_pub_sub_form(sruid),
        topic=Topic(name=OCTUE_SERVICES_PREFIX, project_name=project_name),
        filter=subscription_filter,
        expiration_time=expiration_time,
        push_endpoint=push_endpoint,
    )

    subscription.create(allow_existing=allow_existing)
    return subscription
