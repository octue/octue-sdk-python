import json
import logging
from concurrent.futures import TimeoutError
import google.api_core.exceptions
from google.cloud import pubsub_v1


logger = logging.getLogger(__name__)


GCP_PROJECT = "octue-amy"


class PublisherSubscriber:
    def __init__(self):
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()

    def _initialise_topic(self, topic_name):
        topic_path = self._publisher.topic_path(GCP_PROJECT, topic_name)

        try:
            self._publisher.create_topic(name=topic_path)
        except google.api_core.exceptions.AlreadyExists:
            pass

        return topic_path

    def _initialise_subscription(self, topic, subscription_name):
        subscription_path = self._subscriber.subscription_path(GCP_PROJECT, subscription_name)

        try:
            self._subscriber.create_subscription(name=subscription_path, topic=topic)
        except google.api_core.exceptions.AlreadyExists:
            pass

        return subscription_path

    def _callback(self, response):
        pass


class Service(PublisherSubscriber):
    def __init__(self, name):
        self.name = name
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def ask(self, input_values, input_manifest=None):
        # TODO: Make this a UUID.
        question_topic = self._initialise_topic("test-topic")
        self._publisher.publish(question_topic, json.dumps(input_values).encode())

        # TODO: Make this a UUID.
        response_subscription = self._initialise_subscription(
            topic=self._initialise_topic("test-topic-response"), subscription_name="test-topic-response-subscription",
        )

        streaming_pull_future = self._subscriber.subscribe(response_subscription, callback=self._callback)

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=10)
            except TimeoutError:
                streaming_pull_future.cancel()

        response = vars(self).pop("_response").data
        response = json.loads(response.decode())
        return response

    def respond(self, output_values):
        # TODO: Make this a UUID.
        response_topic = self._initialise_topic("test-topic-response")
        self._publisher.publish(response_topic, json.dumps(output_values).encode())

    def _callback(self, response):
        self._response = response
        response.ack()
