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
        self._topic_path = self._publisher.topic_path(GCP_PROJECT, "test-topic")  # TODO: Replace topic name with a UUID
        self._subscription_path = self._subscriber.subscription_path(
            GCP_PROJECT, "test-subscription"
        )  # TODO: Replace subscription name with a UUID
        self._initialise_topic()
        self._initialise_subscription()

    def _initialise_topic(self):
        try:
            self._publisher.create_topic(name=self._topic_path)
        except google.api_core.exceptions.AlreadyExists:
            pass

    def _initialise_subscription(self):
        try:
            self._subscriber.create_subscription(name=self._subscription_path, topic=self._topic_path)
        except google.api_core.exceptions.AlreadyExists:
            pass

    def _callback(self, response):
        pass


class Service(PublisherSubscriber):
    def __init__(self, name):
        self.name = name
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def ask(self, input_values, input_manifest=None):
        self._publisher.publish(self._topic_path, json.dumps(input_values).encode())
        streaming_pull_future = self._subscriber.subscribe(self._subscription_path, callback=self._callback)

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=10)
            except TimeoutError:
                streaming_pull_future.cancel()

        response = vars(self).pop("_response").data
        response = json.loads(response.decode())
        return response

    def respond(self, output_values):
        self._publisher.publish(self._topic_path, json.dumps(output_values).encode())

    def _callback(self, response):
        self._response = response
        response.ack()
