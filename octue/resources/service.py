import json
import logging
import time
from concurrent.futures import TimeoutError
import google.api_core.exceptions
from google.cloud import pubsub_v1
from icecream import ic


ic.configureOutput(includeContext=True)

logger = logging.getLogger(__name__)


GCP_PROJECT = "octue-amy"


class Service:
    def __init__(self, name):
        self.name = name
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, timeout=None, exit_after_first_response=False):
        publisher = pubsub_v1.PublisherClient()
        topic_name = publisher.topic_path(GCP_PROJECT, self.name)
        publisher.create_topic(name=topic_name)
        ic(topic_name)

        subscriber = pubsub_v1.SubscriberClient()
        subscription_name = subscriber.subscription_path(project=GCP_PROJECT, subscription=self.name)
        subscriber.create_subscription(topic=topic_name, name=subscription_name)
        ic(subscription_name)

        def question_callback(question):
            self._question = question
            question.ack()

        streaming_pull_future = subscriber.subscribe(subscription_name, callback=question_callback)

        with subscriber:
            try:
                ic("Server waiting for questions...")
                streaming_pull_future.result(timeout=20)
            except Exception:
                # streaming_pull_future.cancel()
                pass

        raw_question = vars(self).pop("_question")

        ic(f"Server got question {raw_question.data}.")
        question = json.loads(raw_question.data.decode())  # noqa

        # Insert processing of question here.
        #
        #
        #

        output_values = {}
        self.respond(output_values)

        if exit_after_first_response:
            return

    def respond(self, output_values):
        publisher = pubsub_v1.PublisherClient()
        topic_name = publisher.topic_path(GCP_PROJECT, f"{self.name}-response")

        try:
            publisher.create_topic(name=topic_name)
        except google.api_core.exceptions.AlreadyExists:
            pass

        ic(f"Server responding on topic {topic_name}")
        publisher.publish(topic_name, json.dumps(output_values).encode())
        ic(f"Server responded on topic {topic_name}")

    def ask(self, service_name, input_values, input_manifest=None):
        publisher = pubsub_v1.PublisherClient()
        topic_name = publisher.topic_path(GCP_PROJECT, service_name)
        publisher.publish(topic_name, json.dumps(input_values).encode())
        ic(topic_name)

    def wait_for_response(self, service_name):
        publisher = pubsub_v1.PublisherClient()
        topic_name = publisher.topic_path(GCP_PROJECT, f"{service_name}-response")

        try:
            publisher.create_topic(name=topic_name)
        except google.api_core.exceptions.AlreadyExists:
            pass

        subscriber = pubsub_v1.SubscriberClient()
        subscription_name = subscriber.subscription_path(project=GCP_PROJECT, subscription=f"{service_name}-response")
        subscriber.create_subscription(topic=topic_name, name=subscription_name)

        def callback(response):
            self._response = response
            response.ack()

        ic(f"Asker waiting for response on {topic_name}")
        future = subscriber.subscribe(subscription_name, callback)

        with subscriber:
            try:
                future.result(timeout=20)
            except TimeoutError:
                future.cancel()

        try:
            print(self._response)
            response = vars(self).pop("_response")
        except KeyError:
            pass

        response = json.loads(response.data.decode())
        ic(f"Asker received response: {response}")
        return response

    @staticmethod
    def _time_is_up(start_time, timeout):
        if timeout is None:
            return False

        if time.perf_counter() - start_time < timeout:
            return False

        return True
