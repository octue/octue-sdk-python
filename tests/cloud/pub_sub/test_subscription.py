from unittest.mock import patch
import google.api_core.exceptions

from octue.cloud.pub_sub.service import Service
from octue.cloud.pub_sub.subscription import SEVEN_DAYS, THIRTY_ONE_DAYS, Subscription
from octue.cloud.pub_sub.topic import Topic
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestSubscription(BaseTestCase):

    service = Service(backend=GCPPubSubBackend(project_name="my-project"))
    topic = Topic(name="world", namespace="hello", service=service)
    subscription = Subscription(name="world", topic=topic, namespace="hello", service=service)

    def test_namespace_only_in_name_once(self):
        """Test that the subscription's namespace only appears in its name once, even if it is repeated."""
        self.assertEqual(self.subscription.name, "hello.world")

        subscription_with_repeated_namespace = Subscription(
            name="hello.world", topic=self.topic, namespace="hello", service=self.service
        )

        self.assertEqual(subscription_with_repeated_namespace.name, "hello.world")

    def test_repr(self):
        """Test that Subscriptions are represented correctly."""
        self.assertEqual(repr(self.subscription), "<Subscription(hello.world)>")

    def test_error_raised_when_creating_without_allowing_existing_when_subscription_already_exists(self):
        """Test that an error is raised when trying to create a subscription that already exists and `allow_existing` is
        `False`.
        """
        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.SubscriberClient.create_subscription",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            with self.assertRaises(google.api_core.exceptions.AlreadyExists):
                self.subscription.create(allow_existing=False)

    def test_create_with_allow_existing_when_already_exists(self):
        """Test that trying to create a subscription that already exists when `allow_existing` is `True` results in no
        error.
        """
        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.SubscriberClient.create_subscription",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            self.subscription.create(allow_existing=True)

    def test_create(self):
        """Test that creating a subscription works properly."""
        service = Service(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        topic = Topic(name="my-topic", namespace="tests", service=service)
        subscription = Subscription(name="world", topic=topic, namespace="hello", service=service)

        try:
            topic.create(allow_existing=True)
            response = subscription.create(allow_existing=True)

        finally:
            try:
                subscription.delete()
            finally:
                topic.delete()

        self.assertEqual(response._pb.ack_deadline_seconds, 60)
        self.assertEqual(response._pb.expiration_policy.ttl.seconds, THIRTY_ONE_DAYS)
        self.assertEqual(response._pb.message_retention_duration.seconds, SEVEN_DAYS)
