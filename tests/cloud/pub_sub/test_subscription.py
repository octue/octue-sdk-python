import os
from unittest.mock import patch

import google.api_core.exceptions

from octue.cloud.emulators._pub_sub import MockSubscriber, MockSubscriptionCreationResponse
from octue.cloud.pub_sub.subscription import THIRTY_ONE_DAYS, Subscription
from octue.cloud.pub_sub.topic import Topic
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestSubscription(BaseTestCase):
    topic = Topic(name="world", project_name="my-project", namespace="hello")
    subscription = Subscription(name="world", topic=topic, namespace="hello", project_name=TEST_PROJECT_NAME)

    def test_repr(self):
        """Test that subscriptions are represented correctly."""
        self.assertEqual(repr(self.subscription), "<Subscription(hello.world)>")

    def test_namespace_only_in_name_once(self):
        """Test that the subscription's namespace only appears in its name once, even if it is repeated."""
        self.assertEqual(self.subscription.name, "hello.world")

        subscription_with_repeated_namespace = Subscription(
            name="hello.world",
            topic=self.topic,
            namespace="hello",
            project_name=TEST_PROJECT_NAME,
        )

        self.assertEqual(subscription_with_repeated_namespace.name, "hello.world")

    def test_create_without_allow_existing_when_subscription_already_exists(self):
        """Test that an error is raised when trying to create a subscription that already exists and `allow_existing` is
        `False`.
        """
        with patch("octue.cloud.pub_sub.subscription.SubscriberClient", MockSubscriber):
            subscription = Subscription(
                name="world",
                topic=self.topic,
                namespace="hello",
                project_name=TEST_PROJECT_NAME,
            )

        with patch(
            "octue.cloud.emulators._pub_sub.MockSubscriber.create_subscription",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            with self.assertRaises(google.api_core.exceptions.AlreadyExists):
                subscription.create(allow_existing=False)

        # Check that the subscription creation isn't indicated as being triggered locally.
        self.assertFalse(subscription.creation_triggered_locally)

    def test_create_with_allow_existing_when_already_exists(self):
        """Test that trying to create a subscription that already exists when `allow_existing` is `True` results in no
        error.
        """
        with patch("octue.cloud.pub_sub.subscription.SubscriberClient", MockSubscriber):
            subscription = Subscription(
                name="world",
                topic=self.topic,
                namespace="hello",
                project_name=TEST_PROJECT_NAME,
            )

        with patch(
            "octue.cloud.emulators._pub_sub.MockSubscriber.create_subscription",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            subscription.create(allow_existing=True)

        # Check that the subscription creation isn't indicated as being triggered locally.
        self.assertFalse(subscription.creation_triggered_locally)

    def test_create_pull_subscription(self):
        """Test that creating a pull subscription works properly and that its creation is marked as having been
        triggered locally.
        """
        project_name = os.environ["TEST_PROJECT_NAME"]
        topic = Topic(name="my-topic", project_name=project_name, namespace="tests")

        subscription = Subscription(name="world", topic=topic, namespace="hello", project_name=project_name)

        for allow_existing in (True, False):
            with self.subTest(allow_existing=allow_existing):
                with patch(
                    "google.pubsub_v1.SubscriberClient.create_subscription",
                    new=MockSubscriptionCreationResponse,
                ):
                    response = subscription.create(allow_existing=allow_existing)

                self.assertTrue(subscription.creation_triggered_locally)
                self.assertEqual(response._pb.ack_deadline_seconds, 600)
                self.assertEqual(response._pb.expiration_policy.ttl.seconds, THIRTY_ONE_DAYS)
                self.assertEqual(response._pb.message_retention_duration.seconds, 600)
                self.assertEqual(response._pb.retry_policy.minimum_backoff.seconds, 10)
                self.assertEqual(response._pb.retry_policy.maximum_backoff.seconds, 600)

    def test_create_push_subscription(self):
        """Test that creating a push subscription works properly."""
        project_name = os.environ["TEST_PROJECT_NAME"]
        topic = Topic(name="my-topic", project_name=project_name, namespace="tests")

        subscription = Subscription(
            name="world",
            topic=topic,
            namespace="hello",
            project_name=project_name,
            push_endpoint="https://example.com/endpoint",
        )

        with patch("google.pubsub_v1.SubscriberClient.create_subscription", new=MockSubscriptionCreationResponse):
            response = subscription.create(allow_existing=True)

        self.assertEqual(response._pb.ack_deadline_seconds, 600)
        self.assertEqual(response._pb.expiration_policy.ttl.seconds, THIRTY_ONE_DAYS)
        self.assertEqual(response._pb.message_retention_duration.seconds, 600)
        self.assertEqual(response._pb.retry_policy.minimum_backoff.seconds, 10)
        self.assertEqual(response._pb.retry_policy.maximum_backoff.seconds, 600)
        self.assertEqual(response._pb.push_config.push_endpoint, "https://example.com/endpoint")

    def test_is_pull_subscription(self):
        """Test that `is_pull_subscription` is `True` and `is_push_subscription` is `False` for a pull subscription."""
        self.assertTrue(self.subscription.is_pull_subscription)
        self.assertFalse(self.subscription.is_push_subscription)

    def test_is_push_subscription(self):
        """Test that `is_pull_subscription` is `False` and `is_push_subscription` is `True` for a pull subscription."""
        push_subscription = Subscription(
            name="world",
            topic=self.topic,
            namespace="hello",
            project_name=TEST_PROJECT_NAME,
            push_endpoint="https://example.com/endpoint",
        )

        self.assertTrue(push_subscription.is_push_subscription)
        self.assertFalse(push_subscription.is_pull_subscription)
