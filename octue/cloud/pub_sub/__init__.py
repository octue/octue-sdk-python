from octue.cloud.service_id import convert_service_id_to_pub_sub_form

from .subscription import Subscription
from .topic import Topic


__all__ = ["Subscription", "Topic"]


PARENT_SENDER_TYPE = "PARENT"
CHILD_SENDER_TYPE = "CHILD"
VALID_SENDER_TYPES = {PARENT_SENDER_TYPE, CHILD_SENDER_TYPE}


def create_push_subscription(project_name, sruid, push_endpoint, sender_type, expiration_time=None):
    """Create a Google Pub/Sub push subscription. If a corresponding topic doesn't exist, it will be created.

    :param str project_name: the name of the Google Cloud project in which the subscription will be created
    :param str sruid: the SRUID (service revision unique identifier)
    :param str push_endpoint: the HTTP/HTTPS endpoint of the service to push to. It should be fully formed and include the 'https://' prefix
    :param str sender_type: the type of event to subscribe to (must be one of "PARENT" or "CHILD")
    :param float|None expiration_time: the number of seconds of inactivity after which the subscription should expire. If not provided, no expiration time is applied to the subscription
    """
    if sender_type not in VALID_SENDER_TYPES:
        raise ValueError(f"`sender_type` must be one of {VALID_SENDER_TYPES!r}; received {sender_type!r}")

    pub_sub_sruid = convert_service_id_to_pub_sub_form(sruid)

    topic = Topic(name=pub_sub_sruid, project_name=project_name)
    topic.create(allow_existing=True)

    if expiration_time:
        expiration_time = float(expiration_time)
    else:
        expiration_time = None

    subscription = Subscription(
        name=pub_sub_sruid,
        topic=topic,
        project_name=project_name,
        filter=f'attributes.sender_type = "{sender_type}"',
        expiration_time=expiration_time,
        push_endpoint=push_endpoint,
    )

    subscription.create()
