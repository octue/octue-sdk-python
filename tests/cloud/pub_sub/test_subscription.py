import os
from unittest.mock import patch

import google.api_core.exceptions

from octue.cloud.emulators._pub_sub import MockSubscriber, MockSubscriptionCreationResponse
from octue.cloud.pub_sub.subscription import THIRTY_ONE_DAYS, Subscription
from octue.cloud.pub_sub.topic import Topic
from tests import TEST_PROJECT_ID
from tests.base import BaseTestCase


class TestSubscription(BaseTestCase):
    topic = Topic(name="world", project_id=TEST_PROJECT_ID)
    subscription = Subscription(name="world", topic=topic)

    def test_repr(self):
        """Test that subscriptions are represented correctly."""
        self.assertEqual(repr(self.subscription), "<Subscription(name='world', filter=None)>")

    def test_instantiating_without_credentials(self):
        """Test that a subscription can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Subscription(name="world", topic=self.topic)

    def test_create_without_allow_existing_when_subscription_already_exists(self):
        """Test that an error is raised when trying to create a subscription that already exists and `allow_existing` is
        `False`.
        """
        subscription = Subscription(name="world", topic=self.topic)

        with patch("octue.cloud.pub_sub.subscription.SubscriberClient", MockSubscriber):
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
        subscription = Subscription(name="world", topic=self.topic)

        with patch("octue.cloud.pub_sub.subscription.SubscriberClient", MockSubscriber):
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
        topic = Topic(name="my-topic", project_id=TEST_PROJECT_ID)
        subscription = Subscription(name="world", topic=topic, filter='attributes.question_uuid = "abc"')

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
                self.assertTrue(response._pb.enable_message_ordering)
                self.assertEqual(response._pb.retry_policy.minimum_backoff.seconds, 10)
                self.assertEqual(response._pb.retry_policy.maximum_backoff.seconds, 600)

    def test_is_pull_subscription(self):
        """Test that `is_pull_subscription` is `True` for a pull subscription."""
        self.assertTrue(self.subscription.is_pull_subscription)
        self.assertFalse(self.subscription.is_push_subscription)

    def test_is_push_subscription(self):
        """Test that `is_push_subscription` is `True` for a push subscription."""
        push_subscription = Subscription(name="world", topic=self.topic, push_endpoint="https://example.com/endpoint")
        self.assertTrue(push_subscription.is_push_subscription)
        self.assertFalse(push_subscription.is_pull_subscription)
