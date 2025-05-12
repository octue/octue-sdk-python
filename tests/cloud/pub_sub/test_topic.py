import os
from unittest.mock import patch

import google.api_core.exceptions

from octue.cloud.pub_sub.topic import Topic
from tests.base import BaseTestCase


class TestTopic(BaseTestCase):
    def test_repr(self):
        """Test that Topics are represented correctly."""
        topic = Topic(name="world", project_id="my-project")
        self.assertEqual(repr(topic), "<Topic(name='world')>")

    def test_instantiating_without_credentials(self):
        """Test that a topic can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Topic(name="world", project_id="my-project")

    def test_create(self):
        """Test that a topic can be created and that it's marked as having its creation triggered locally."""
        topic = Topic(name="world", project_id="my-project")

        for allow_existing in (True, False):
            with self.subTest(allow_existing=allow_existing):
                with patch("octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.create_topic"):
                    topic.create(allow_existing=allow_existing)

                self.assertTrue(topic.creation_triggered_locally)

    def test_create_without_allow_existing_when_already_exists(self):
        """Test that an error is raised when trying to create a topic that already exists and `allow_existing` is
        `False`.
        """
        topic = Topic(name="world", project_id="my-project")

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.create_topic",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            with self.assertRaises(google.api_core.exceptions.AlreadyExists):
                topic.create(allow_existing=False)

        # Check that the topic creation isn't indicated as being triggered locally.
        self.assertFalse(topic.creation_triggered_locally)

    def test_create_with_allow_existing_when_already_exists(self):
        """Test that trying to create a topic that already exists when `allow_existing` is `True` results in no error."""
        topic = Topic(name="world", project_id="my-project")

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.create_topic",
            side_effect=google.api_core.exceptions.AlreadyExists(""),
        ):
            topic.create(allow_existing=True)

        # Check that the topic creation isn't indicated as being triggered locally.
        self.assertFalse(topic.creation_triggered_locally)

    def test_exists(self):
        """Test that topics can be tested for existence."""
        topic = Topic(name="world", project_id="my-project")

        with patch("octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.get_topic"):
            self.assertTrue(topic.exists())

        with patch(
            "octue.cloud.pub_sub.service.pubsub_v1.PublisherClient.get_topic",
            side_effect=google.api_core.exceptions.NotFound(""),
        ):
            self.assertFalse(topic.exists(timeout=1))

    def test_exists_returns_false_if_timeout_exceeded(self):
        """Test `Topic.exists` returns `False` if the timeout is exceeded."""
        topic = Topic(name="world", project_id="my-project")
        self.assertFalse(topic.exists(timeout=0))
