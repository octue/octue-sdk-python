from unittest.mock import patch
import google.api_core.exceptions

from octue.cloud.pub_sub.service import Service
from octue.cloud.pub_sub.topic import Topic
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


class TestTopic(BaseTestCase):

    service = Service(backend=GCPPubSubBackend(project_name="my-project"))

    def test_namespace_only_in_name_once(self):
        """Test that the topic's namespace only appears in its name once, even if it is repeated."""
        topic = Topic(name="world", namespace="hello", service=self.service)
        self.assertEqual(topic.name, "hello.world")

        topic_with_repeated_namespace = Topic(name="hello.world", namespace="hello", service=self.service)
        self.assertEqual(topic_with_repeated_namespace.name, "hello.world")

    def test_repr(self):
        """Test that Topics are represented correctly."""
        self.assertEqual(repr(Topic(name="world", namespace="hello", service=self.service)), "<Topic(hello.world)>")

    def test_error_raised_when_creating_without_allowing_existing_when_topic_already_exists(self):
        """Test that an error is raised when trying to create a topic that already exists and `allow_existing` is
        `False`.
        """
        topic = Topic(name="world", namespace="hello", service=self.service)

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.create_topic",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            with self.assertRaises(google.api_core.exceptions.AlreadyExists):
                topic.create(allow_existing=False)

    def test_create_with_allow_existing_when_already_exists(self):
        """Test that trying to create a topic that already exists when `allow_existing` is `True` results in no error."""
        topic = Topic(name="world", namespace="hello", service=self.service)

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.create_topic",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            topic.create(allow_existing=True)

    def test_exists(self):
        """Test that topics can be tested for existence."""
        topic = Topic(name="world", namespace="hello", service=self.service)

        with patch("octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.get_topic"):
            self.assertTrue(topic.exists())

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.get_topic",
            side_effect=google.api_core.exceptions.NotFound(""),
        ):
            self.assertFalse(topic.exists())
