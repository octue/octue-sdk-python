import time
from tests.base import BaseTestCase

from octue.resources.service import GCP_PROJECT, PublisherSubscriber, Service


class TestPublisherSubscriber(BaseTestCase):
    pub_sub = PublisherSubscriber()

    def test_initialise_topic(self):
        """ Test a topic can be initialised. """
        topic = self.pub_sub._initialise_topic("blah")
        self.assertEqual(topic, "/".join(("projects", GCP_PROJECT, "topics", "blah")))

    def test_initialise_existing_topic(self):
        """ Test that initialising an existing topic returns the existing topic. """
        topic_1 = self.pub_sub._initialise_topic("blah")
        topic_2 = self.pub_sub._initialise_topic("blah")
        self.assertEqual(topic_1, topic_2)

    def test_initialise_subscription(self):
        """ Test a subscription can be initialised. """
        topic = self.pub_sub._initialise_topic("blah")
        subscription = self.pub_sub._initialise_subscription(topic, "blah-sub")
        self.assertEqual(subscription, "/".join(("projects", GCP_PROJECT, "subscriptions", "blah-sub")))

    def test_initialise_existing_subscription(self):
        """ Test that initialising an existing subscription returns the existing subscription. """
        topic = self.pub_sub._initialise_topic("blah")
        subscription_1 = self.pub_sub._initialise_subscription(topic, "blah-sub")
        subscription_2 = self.pub_sub._initialise_subscription(topic, "blah-sub")
        self.assertEqual(subscription_1, subscription_2)


class TestService(BaseTestCase):
    def test_serve_with_timeout(self):
        """ Test that a Service can serve for a given time interval and stop at the end of it. """
        service = Service(name="hello")
        start_time = time.perf_counter()
        service.serve(timeout=0)
        duration = time.perf_counter() - start_time
        self.assertTrue(duration < 5)  # Allow for time spent connecting to Google.
