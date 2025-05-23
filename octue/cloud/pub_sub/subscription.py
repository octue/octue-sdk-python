import logging
from functools import cached_property

import google.api_core.exceptions
from google.cloud.pubsub_v1 import SubscriberClient
from google.protobuf.duration_pb2 import Duration  # noqa
from google.protobuf.field_mask_pb2 import FieldMask  # noqa
from google.pubsub_v1.types.pubsub import (
    ExpirationPolicy,
    PushConfig,
    RetryPolicy,
    Subscription as _Subscription,
    UpdateSubscriptionRequest,
)


logger = logging.getLogger(__name__)

# Useful time periods in seconds.
THIRTY_ONE_DAYS = 31 * 24 * 3600


class Subscription:
    """A candidate subscription to use with Google Pub/Sub. The subscription represented by an instance of this class
    does not necessarily already exist on the Google Pub/Sub servers.

    :param str name: the name of the subscription excluding "projects/<project_id>/subscriptions/<namespace>"
    :param octue.cloud.pub_sub.topic.Topic topic: the topic the subscription is attached to
    :param str|None project_id: the ID of the Google Cloud project that the subscription belongs to; if `None`, the project ID of the topic is used
    :param str|None filter: if provided, only receive messages matching the filter (see here for filter syntax: https://cloud.google.com/pubsub/docs/subscription-message-filter#filtering_syntax)
    :param int ack_deadline: the time in seconds after which, if the subscriber hasn't acknowledged a message, to retry sending it to the subscription
    :param int message_retention_duration: unacknowledged message retention time in seconds
    :param int|float|None expiration_time: number of seconds of inactivity after which the subscription is deleted (infinite time if `None`)
    :param float minimum_retry_backoff: minimum number of seconds after the acknowledgement deadline has passed to exponentially retry delivering a message to the subscription
    :param float maximum_retry_backoff: maximum number of seconds after the acknowledgement deadline has passed to exponentially retry delivering a message to the subscription
    :param str|None push_endpoint: if this is a push subscription, this is the URL to which messages should be pushed; leave as `None` if it's not a push subscription
    :param bool enable_message_ordering: if `True`, receive messages with the same ordering key in the order they were published
    :return None:
    """

    def __init__(
        self,
        name,
        topic,
        project_id=None,
        filter=None,
        ack_deadline=600,
        message_retention_duration=600,
        expiration_time=THIRTY_ONE_DAYS,
        minimum_retry_backoff=10,
        maximum_retry_backoff=600,
        push_endpoint=None,
        enable_message_ordering=True,
    ):
        self.name = name
        self.topic = topic
        self.filter = filter
        self.path = self.generate_subscription_path(project_id or self.topic.project_id, self.name)
        self.ack_deadline = ack_deadline
        self.message_retention_duration = Duration(seconds=message_retention_duration)

        # If expiration_time is `None`, the subscription will never expire.
        if expiration_time is None:
            self.expiration_policy = ExpirationPolicy(mapping=None)
        else:
            self.expiration_policy = ExpirationPolicy(mapping=None, ttl=Duration(seconds=expiration_time))

        self.retry_policy = RetryPolicy(
            mapping=None,
            minimum_backoff=Duration(seconds=minimum_retry_backoff),
            maximum_backoff=Duration(seconds=maximum_retry_backoff),
        )

        self.push_endpoint = push_endpoint
        self.enable_message_ordering = enable_message_ordering
        self._created = False

    @cached_property
    def subscriber(self):
        """Get or instantiate the subscriber client. The client isn't instantiated until this property is called for the
        first time. This allows checking for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to be put off
        until it's needed.

        :return google.cloud.pubsub_v1.SubscriberClient:
        """
        return SubscriberClient()

    @property
    def creation_triggered_locally(self):
        """Was the subscription successfully created by calling `self.create` locally? This is `False` if its creation
        was triggered remotely.

        :return bool:
        """
        return self._created

    @property
    def is_pull_subscription(self):
        """Return `True` if this is a pull subscription.

        :return bool:
        """
        return self.push_endpoint is None

    @property
    def is_push_subscription(self):
        """Return `True` if this is a push subscription.

        :return bool:
        """
        return self.push_endpoint is not None

    def __repr__(self):
        """Represent the subscription as a string.

        :return str:
        """
        return f"<{type(self).__name__}(name={self.name!r}, filter={self.filter!r})>"

    def create(self, allow_existing=False):
        """Create a Google Pub/Sub subscription that can be subscribed to.

        :param bool allow_existing: if `False`, raise an error if the subscription already exists; if `True`, do nothing (the existing subscription is not overwritten)
        :return google.pubsub_v1.types.pubsub.Subscription:
        """
        subscription = self._create_proto_message_subscription()

        if not allow_existing:
            subscription = self.subscriber.create_subscription(request=subscription)
            self._created = True
            self._log_creation()
            return subscription

        try:
            subscription = self.subscriber.create_subscription(request=subscription)
            self._created = True
        except google.api_core.exceptions.AlreadyExists:
            pass

        self._log_creation()
        return subscription

    def update(self):
        """Update an existing subscription with the state of this instance.

        :return None:
        """
        self.subscriber.update_subscription(
            request=UpdateSubscriptionRequest(
                mapping=None,
                subscription=self._create_proto_message_subscription(),  # noqa
                update_mask=FieldMask(
                    paths=[
                        "ack_deadline_seconds",
                        "message_retention_duration",
                        "enable_message_ordering",
                        "expiration_policy",
                        "retry_policy",
                    ]
                ),
            )
        )

    def delete(self):
        """Delete the subscription from Google Pub/Sub.

        :return None:
        """
        self.subscriber.delete_subscription(subscription=self.path)
        logger.info("Subscription %r deleted.", self.path)

    def exists(self, timeout=5):
        """Check if the subscription exists on the Google Pub/Sub servers.

        :param float timeout:
        :return bool:
        """
        try:
            self.subscriber.get_subscription(subscription=self.path, timeout=timeout)
            return True
        except google.api_core.exceptions.NotFound:
            return False

    @staticmethod
    def generate_subscription_path(project_id, subscription_name):
        """Generate a full subscription path in the format `projects/<project_id>/subscriptions/<subscription_name>`.

        :param str project_id:
        :param str subscription_name:
        :return str:
        """
        return f"projects/{project_id}/subscriptions/{subscription_name}"

    def _create_proto_message_subscription(self):
        """Create a Proto message subscription from the instance to be sent to the Pub/Sub API.

        :return google.pubsub_v1.types.pubsub.Subscription:
        """
        if self.push_endpoint:
            options = {"push_config": PushConfig(mapping=None, push_endpoint=self.push_endpoint)}  # noqa
        else:
            options = {}

        return _Subscription(
            mapping=None,
            name=self.path,  # noqa
            topic=self.topic.path,
            filter=self.filter,  # noqa
            ack_deadline_seconds=self.ack_deadline,  # noqa
            message_retention_duration=self.message_retention_duration,  # noqa
            enable_message_ordering=self.enable_message_ordering,  # noqa
            expiration_policy=self.expiration_policy,  # noqa
            retry_policy=self.retry_policy,  # noqa
            **options,
        )

    def _log_creation(self):
        """Log the creation of the subscription.

        :return None:
        """
        logger.debug("Subscription %r created.", self.path)
