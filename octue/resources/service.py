import json
import logging
import time
import uuid
from concurrent.futures import TimeoutError
import google.api_core.exceptions
from google.cloud import pubsub_v1


logger = logging.getLogger(__name__)


OCTUE_NAMESPACE = "octue.services"


class Topic:

    # Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
    # microservices publishing single messages in a request-response sequence.
    BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)

    def __init__(self, name, gcp_project_name):
        self.name = name
        self._publisher = pubsub_v1.PublisherClient(self.BATCH_SETTINGS)
        self.path = self._publisher.topic_path(gcp_project_name, f"{OCTUE_NAMESPACE}.{self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delete()

    def create(self, allow_existing=False):
        if not allow_existing:
            self._publisher.create_topic(name=self.path)
            logger.debug("Created topic %r.", self.path)

        else:
            try:
                self._publisher.create_topic(name=self.path)
                logger.debug("Created topic %r.", self.path)
            except google.api_core.exceptions.AlreadyExists:
                pass

    def publish(self, data, blocking, **attributes):
        future = self._publisher.publish(self.path, data, **attributes)

        if blocking:
            future.result()

    def delete(self):
        self._publisher.stop()
        self._publisher.delete_topic(topic=self.path)
        logger.debug("Deleted topic %r and stopped publisher.", self.path)


class Subscription:
    def __init__(self, name, topic, gcp_project_name):
        self.name = name
        self.topic = topic
        self.subscriber = pubsub_v1.SubscriberClient()
        self.path = self.subscriber.subscription_path(gcp_project_name, f"{OCTUE_NAMESPACE}.{self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delete()

    def create(self, allow_existing=False):
        if not allow_existing:
            self.subscriber.create_subscription(topic=self.topic.path, name=self.path)
            logger.debug("Created subscription %r.", self.path)

        else:
            try:
                self.subscriber.create_subscription(topic=self.topic.path, name=self.path)
                logger.debug("Created subscription %r.", self.path)
            except google.api_core.exceptions.AlreadyExists:
                pass

    def subscribe(self, callback=None):
        return self.subscriber.subscribe(self.path, callback=callback)

    def delete(self):
        self.subscriber.delete_subscription(subscription=self.path)
        self.subscriber.close()
        logger.debug("Deleted subscription %r and closed subscriber.", self.path)


class Service:
    def __init__(self, name, gcp_project_name):
        self.name = name
        self.gcp_project_name = gcp_project_name

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, run_function, timeout=None):

        questions = []

        with Topic(name=self.name, gcp_project_name=self.gcp_project_name) as topic:
            topic.create()

            with Subscription(name=self.name, topic=topic, gcp_project_name=self.gcp_project_name) as subscription:
                subscription.create()

                def question_callback(question):
                    questions.append(question)
                    question.ack()

                streaming_pull_future = subscription.subscribe(callback=question_callback)
                start_time = time.perf_counter()

                logger.debug("%r server is waiting for questions.", self)
                while True:

                    if self._time_is_up(start_time, timeout):
                        streaming_pull_future.cancel()
                        raise TimeoutError(f"{self!r} ran out of time waiting for questions.")

                    try:
                        streaming_pull_future.result(timeout=10)
                    except TimeoutError:
                        pass

                    if not questions:
                        continue

                    logger.info("%r received %d questions.", self, len(questions))

                    for question in questions:
                        data = json.loads(question.data.decode())
                        analysis = run_function(data)
                        self.respond(question_uuid=question.attributes["uuid"], output_values=analysis.output_values)

                    questions = []

    def respond(self, question_uuid, output_values):
        topic = Topic(name=f"{self.name}.response.{question_uuid}", gcp_project_name=self.gcp_project_name)
        topic.publish(data=json.dumps(output_values).encode(), blocking=False)
        logger.info("%r responded on topic %r.}", self, topic.path)

    def ask(self, service_name, input_values, input_manifest=None):
        question_uuid = str(int(uuid.uuid4()))
        response_topic_and_subscription_name = f"{service_name}.response.{question_uuid}"

        response_topic = Topic(name=response_topic_and_subscription_name, gcp_project_name=self.gcp_project_name)
        response_topic.create(allow_existing=False)

        response_subscription = Subscription(
            name=response_topic_and_subscription_name, topic=response_topic, gcp_project_name=self.gcp_project_name
        )
        response_subscription.create(allow_existing=False)

        def answer_callback(answer):
            self._answer = answer
            answer.ack()

        future = response_subscription.subscribe(callback=answer_callback)

        question_topic = Topic(name=service_name, gcp_project_name=self.gcp_project_name)
        question_topic.publish(data=json.dumps(input_values).encode(), uuid=question_uuid, blocking=True)
        logger.debug("%r asked question to %r service. Question UUID is %r.", self, service_name, question_uuid)

        return future, response_subscription

    def wait_for_answer(self, future, subscription, timeout=20):
        try:
            future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()

        try:
            answer = vars(self).pop("_answer")
        except KeyError:
            raise TimeoutError(f"{self} timed out waiting for an answer.")

        answer = json.loads(answer.data.decode())
        logger.debug("%r received a response to question on topic %r", self, subscription.topic)
        subscription.delete()
        return answer

    @staticmethod
    def _time_is_up(start_time, timeout):
        if timeout is None:
            return False

        if time.perf_counter() - start_time < timeout:
            return False

        return True
