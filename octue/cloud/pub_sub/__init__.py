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
    subscription_suffix=None,
):
    """Create a Google Pub/Sub push subscription for an Octue service for it to receive questions from parents. If a
    corresponding topic doesn't exist, it will be created first.

    :param str project_name: the name of the Google Cloud project in which the subscription will be created
    :param str sruid: the SRUID (service revision unique identifier)
    :param str push_endpoint: the HTTP/HTTPS endpoint of the service to push to. It should be fully formed and include the 'https://' prefix
    :param str|None subscription_filter: if specified, the filter to apply to the subscription; otherwise, no filter is applied
    :param float|None expiration_time: the number of seconds of inactivity after which the subscription should expire. If not provided, no expiration time is applied to the subscription
    :param str|None subscription_suffix: if provided, add a suffix to the end of the subscription name. This is useful when needing to create multiple subscriptions for the same topic (subscription names are unique).
    :return None:
    """
    topic_name = convert_service_id_to_pub_sub_form(sruid)

    if subscription_suffix:
        subscription_name = topic_name + subscription_suffix
    else:
        subscription_name = topic_name

    topic = Topic(name=topic_name, project_name=project_name)
    topic.create(allow_existing=True)

    if expiration_time:
        expiration_time = float(expiration_time)
    else:
        expiration_time = None

    subscription = Subscription(
        name=subscription_name,
        topic=topic,
        project_name=project_name,
        filter=subscription_filter,
        expiration_time=expiration_time,
        push_endpoint=push_endpoint,
    )

    subscription.create()
