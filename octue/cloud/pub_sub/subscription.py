import logging
import google.api_core.exceptions
from google.protobuf.duration_pb2 import Duration
from google.pubsub_v1.types.pubsub import ExpirationPolicy, Subscription as _Subscription


logger = logging.getLogger(__name__)

# Useful time periods in seconds.
SEVEN_DAYS = 7 * 24 * 3600
THIRTY_ONE_DAYS = 31 * 24 * 3600


class Subscription:
    """A candidate subscription to use with Google Pub/Sub. The subscription represented by an instance of this class
    does not necessarily already exist on the Google Pub/Sub servers.

    :param str name: the name of the subscription excluding "projects/<project_name>/subscriptions/<namespace>"
    :param octue.cloud.pub_sub.topic.Topic topic: the topic the subscription is attached to
    :param str namespace: a namespace to put before the subscription's name in its path
    :param octue.cloud.pub_sub.service.Service service: the service using this subscription
    :param int ack_deadline: message acknowledgement deadline in seconds
    :param int message_retention_duration: unacknowledged message retention time in seconds
    :param int|None expiration_time: number of seconds after which the subscription is deleted (infinite time if None)
    :return None:
    """

    def __init__(
        self,
        name,
        topic,
        namespace,
        service,
        ack_deadline=60,
        message_retention_duration=SEVEN_DAYS,
        expiration_time=THIRTY_ONE_DAYS,
    ):
        if name.startswith(namespace):
            self.name = name
        else:
            self.name = f"{namespace}.{name}"

        self.topic = topic
        self.service = service
        self.path = self.service.subscriber.subscription_path(self.service.backend.project_name, self.name)
        self.ack_deadline = ack_deadline
        self.message_retention_duration = Duration(seconds=message_retention_duration)

        # If expiration_time is None, the subscription will never expire.
        if expiration_time is None:
            self.expiration_policy = ExpirationPolicy(mapping=None)
        else:
            self.expiration_policy = ExpirationPolicy(mapping=None, ttl=Duration(seconds=expiration_time))

    def __repr__(self):
        return f"<{type(self).__name__}({self.name})>"

    def create(self, allow_existing=False):
        """Create a Google Pub/Sub subscription that can be subscribed to.

        :param bool allow_existing: if `False`, raise an error if the subscription already exists; if `True`, do nothing (the existing subscription is not overwritten)
        :return google.pubsub_v1.types.pubsub.Subscription:
        """
        subscription = _Subscription(
            mapping=None,
            name=self.path,
            topic=self.topic.path,
            ack_deadline_seconds=self.ack_deadline,  # noqa
            message_retention_duration=self.message_retention_duration,  # noqa
            expiration_policy=self.expiration_policy,  # noqa
        )

        if not allow_existing:
            self.service.subscriber.create_subscription(request=subscription)
            self._log_creation()
            return

        try:
            self.service.subscriber.create_subscription(request=subscription)
        except google.api_core.exceptions.AlreadyExists:
            pass

        self._log_creation()
        return subscription

    def delete(self):
        """ Delete the subscription from Google Pub/Sub. """
        self.service.subscriber.delete_subscription(subscription=self.path)
        logger.debug("%r deleted subscription %r.", self.service, self.path)

    def _log_creation(self):
        """ Log the creation of the subscription. """
        logger.debug("%r created subscription %r.", self.service, self.path)
