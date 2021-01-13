import json
import logging
import uuid
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

    def serve(self, timeout=None):
        serving_topic = self._initialise_topic(self.name)

        serving_subscription = self._initialise_subscription(
            topic=serving_topic, subscription_name=f"{self.name}-subscription",
        )

        def question_callback(question):
            self._question = question

        streaming_pull_future = self._subscriber.subscribe(serving_subscription, callback=question_callback)

        while True:
            with self._subscriber:
                try:
                    streaming_pull_future.result(timeout=5)
                except TimeoutError:
                    streaming_pull_future.cancel()

            try:
                raw_question = vars(self).pop("_question")
            except KeyError:
                continue

            question = json.loads(raw_question.data.decode())  # noqa
            question_uuid = raw_question.args["uuid"]

            # Insert processing of question here.
            #
            #
            #

            output_values = {}
            self.respond(question_uuid, output_values)

    def ask(self, service_name, input_values, input_manifest=None, timeout=60):
        question_topic = self._initialise_topic(service_name)
        question_uuid = json.dumps(str(uuid.uuid4()))

        self._publisher.publish(question_topic, json.dumps(input_values).encode(), uuid=question_uuid)

        response_subscription = self._initialise_subscription(
            topic=self._initialise_topic(f"{service_name}-response-{question_uuid}"),
            subscription_name=f"{service_name}-response-subscription",
        )

        streaming_pull_future = self._subscriber.subscribe(response_subscription, callback=self._callback)

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()

        response = vars(self).pop("_response").data
        response = json.loads(response.decode())
        return response

    def respond(self, question_uuid, output_values):
        response_topic = self._initialise_topic(f"{self.name}-response-{question_uuid}")
        self._publisher.publish(response_topic, json.dumps(output_values).encode())

    def _callback(self, response):
        self._response = response
        response.ack()
