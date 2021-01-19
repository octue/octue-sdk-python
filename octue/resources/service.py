import json
import logging
import sys
import time
from concurrent.futures import TimeoutError
from signal import SIGINT, signal
import google.api_core.exceptions
from google.cloud import pubsub_v1
from icecream import ic


ic.configureOutput(includeContext=True)

logger = logging.getLogger(__name__)


GCP_PROJECT = "octue-amy"


def delete_all_topics_and_subscriptions():
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    for topic in publisher.list_topics(project=f"projects/{GCP_PROJECT}"):
        publisher.delete_topic(topic=topic.name)
        print(f"Deleted {topic.name}")

    with subscriber:
        for subscription in subscriber.list_subscriptions(project=f"projects/{GCP_PROJECT}"):
            subscriber.delete_subscription(subscription=subscription.name)
            print(f"Deleted {subscription.name}")


class Topic:
    def __init__(self, name):
        self.name = name
        self._publisher = pubsub_v1.PublisherClient()
        self.path = self._publisher.topic_path(GCP_PROJECT, self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._publisher.delete_topic(topic=self.path)

    def create(self):
        self._publisher.create_topic(name=self.path)


class Subscription:
    def __init__(self, name, topic):
        self.name = name
        self.topic = topic
        self.subscriber = pubsub_v1.SubscriberClient()
        self.path = self.subscriber.subscription_path(GCP_PROJECT, self.name)

    def __enter__(self):
        signal(SIGINT, self._sigint_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.subscriber.delete_subscription(subscription=self.path)

    def create(self):
        self.subscriber.create_subscription(topic=self.topic.path, name=self.path)

    def subscribe(self, callback=None):
        return self.subscriber.subscribe(self.path, callback=callback)

    def _sigint_handler(self, signal_received, frame):
        self.__exit__(None, None, None)
        sys.exit(0)


class Service:
    def __init__(self, name):
        self.name = name
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, timeout=None, exit_after_first_response=False):

        with Topic(name=self.name) as topic:
            topic.create()
            ic(topic.path)

            with Subscription(name=self.name, topic=topic) as subscription:
                subscription.create()
                ic(subscription.path)

                def question_callback(question):
                    self._question = question
                    question.ack()

                streaming_pull_future = subscription.subscribe(callback=question_callback)

                with subscription.subscriber:
                    start_time = time.perf_counter()

                    while True:
                        if self._time_is_up(start_time, timeout):
                            return

                        try:
                            ic("Server waiting for questions...")
                            streaming_pull_future.result(timeout=20)
                        except Exception:
                            # streaming_pull_future.cancel()
                            pass

                        try:
                            raw_question = vars(self).pop("_question")
                        except KeyError:
                            continue

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

    def wait_for_response(self, service_name, timeout=20):
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
                future.result(timeout=timeout)
            except TimeoutError:
                future.cancel()

            try:
                print(self._response)
                response = vars(self).pop("_response")
            except KeyError:
                pass

            subscriber.delete_subscription(subscription=subscription_name)

        publisher.delete_topic(topic=topic_name)

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
