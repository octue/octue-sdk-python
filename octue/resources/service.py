import json
import logging
import time
import uuid
from concurrent.futures import TimeoutError
import google.api_core.exceptions
from google.cloud import pubsub_v1
from icecream import ic


ic.configureOutput(includeContext=True)

logger = logging.getLogger(__name__)


GCP_PROJECT = "octue-amy"


class Topic:
    def __init__(self, name, delete_on_exit=True):
        self.name = name
        self._delete_on_exit = delete_on_exit
        self._publisher = pubsub_v1.PublisherClient()
        self.path = self._publisher.topic_path(GCP_PROJECT, self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._delete_on_exit:
            self._publisher.delete_topic(topic=self.path)

    def create(self, allow_existing=False):
        if not allow_existing:
            self._publisher.create_topic(name=self.path)

        else:
            try:
                self._publisher.create_topic(name=self.path)
            except google.api_core.exceptions.AlreadyExists:
                pass

    def publish(self, data, **attributes):
        self._publisher.publish(self.path, data, **attributes)


class Subscription:
    def __init__(self, name, topic):
        self.name = name
        self.topic = topic
        self.subscriber = pubsub_v1.SubscriberClient()
        self.path = self.subscriber.subscription_path(GCP_PROJECT, self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.subscriber.delete_subscription(subscription=self.path)
        self.subscriber.close()

    def create(self, allow_existing=False):
        if not allow_existing:
            self.subscriber.create_subscription(topic=self.topic.path, name=self.path)

        else:
            try:
                self.subscriber.create_subscription(topic=self.topic.path, name=self.path)
            except google.api_core.exceptions.AlreadyExists:
                pass

    def subscribe(self, callback=None):
        return self.subscriber.subscribe(self.path, callback=callback)


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
                    self.respond(question_uuid=raw_question.attributes["uuid"], output_values=output_values)

                    if exit_after_first_response:
                        return

    def respond(self, question_uuid, output_values):
        with Topic(name=f"{self.name}-response-{question_uuid}", delete_on_exit=False) as topic:
            topic.create(allow_existing=True)
            topic.publish(json.dumps(output_values).encode())
            ic(f"Server responded on topic {topic.path} to UUID {question_uuid}")

    def ask(self, service_name, input_values, input_manifest=None):
        with Topic(name=service_name, delete_on_exit=False) as topic:
            question_uuid = str(int(uuid.uuid4()))
            topic.publish(json.dumps(input_values).encode(), uuid=question_uuid)
            ic(topic.path)
            ic(question_uuid)
            return question_uuid

    def wait_for_response(self, question_uuid, service_name, timeout=20):
        with Topic(name=f"{service_name}-response-{question_uuid}") as topic:
            topic.create(allow_existing=True)

            with Subscription(name=f"{service_name}-response-{question_uuid}", topic=topic) as subscription:
                subscription.create()

                def callback(response):
                    self._response = response
                    response.ack()

                ic(f"Asker waiting for response on {topic.path}")
                future = subscription.subscribe(callback=callback)

                try:
                    future.result(timeout=timeout)
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
