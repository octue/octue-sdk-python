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
    def __init__(self, name, gcp_project_name, delete_on_exit=False):
        self.name = name
        self._delete_on_exit = delete_on_exit
        self._publisher = pubsub_v1.PublisherClient()
        self.path = self._publisher.topic_path(gcp_project_name, f"{OCTUE_NAMESPACE}.{self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._delete_on_exit:
            self._publisher.delete_topic(topic=self.path)
            logger.debug("Deleted topic %r.", self.path)

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

    def publish(self, data, **attributes):
        self._publisher.publish(self.path, data, **attributes)


class Subscription:
    def __init__(self, name, topic, gcp_project_name, delete_on_exit=False):
        self.name = name
        self.topic = topic
        self.delete_on_exit = delete_on_exit
        self.subscriber = pubsub_v1.SubscriberClient()
        self.path = self.subscriber.subscription_path(gcp_project_name, f"{OCTUE_NAMESPACE}.{self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.delete_on_exit:
            self.subscriber.delete_subscription(subscription=self.path)
            logger.debug("Deleted subscription %r.", self.path)
        self.subscriber.close()

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


class Service:
    def __init__(self, name, gcp_project_name):
        self.name = name
        self.gcp_project_name = gcp_project_name

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, run_function, timeout=None):

        questions = []

        with Topic(name=self.name, gcp_project_name=self.gcp_project_name, delete_on_exit=True) as topic:
            topic.create()

            with Subscription(
                name=self.name, topic=topic, gcp_project_name=self.gcp_project_name, delete_on_exit=True
            ) as subscription:
                subscription.create()

                def question_callback(question):
                    questions.append(question)
                    question.ack()

                streaming_pull_future = subscription.subscribe(callback=question_callback)
                start_time = time.perf_counter()

                while True:
                    logger.debug("%r server is waiting for questions.", self)

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
        with Topic(name=f"{self.name}.response.{question_uuid}", gcp_project_name=self.gcp_project_name) as topic:
            topic.create(allow_existing=True)
            topic.publish(json.dumps(output_values).encode())
            logger.info("%r responded on topic %r to question UUID %s.}", self, topic.path, question_uuid)

    def ask(self, service_name, input_values, input_manifest=None):
        with Topic(name=service_name, gcp_project_name=self.gcp_project_name) as topic:
            question_uuid = str(int(uuid.uuid4()))
            topic.publish(json.dumps(input_values).encode(), uuid=question_uuid)
            logger.debug("%r asked question to %r service. Question UUID is %r.", self, service_name, question_uuid)
            return question_uuid

    def wait_for_answer(self, question_uuid, service_name, timeout=20):
        with Topic(
            name=f"{service_name}.response.{question_uuid}", gcp_project_name=self.gcp_project_name, delete_on_exit=True
        ) as topic:
            topic.create(allow_existing=True)

            with Subscription(
                name=f"{service_name}.response.{question_uuid}",
                topic=topic,
                gcp_project_name=self.gcp_project_name,
                delete_on_exit=True,
            ) as subscription:
                subscription.create()

                def callback(response):
                    self._response = response
                    response.ack()

                future = subscription.subscribe(callback=callback)

                start_time = time.perf_counter()
                while True:
                    if self._time_is_up(start_time, timeout=timeout):
                        future.cancel()
                        raise TimeoutError(
                            f"Ran out of time to wait for response from service {service_name!r} for question "
                            f"{question_uuid}."
                        )

                    logger.debug(
                        "%r is waiting for a response to question %r from service %r.",
                        self,
                        question_uuid,
                        service_name,
                    )

                    try:
                        future.result(timeout=5)
                    except TimeoutError:
                        pass

                    try:
                        response = vars(self).pop("_response")
                        break
                    except KeyError:
                        pass

        response = json.loads(response.data.decode())
        logger.debug("%r received a response to question %r from service %r.", self, question_uuid, service_name)
        return response

    @staticmethod
    def _time_is_up(start_time, timeout):
        if timeout is None:
            return False

        if time.perf_counter() - start_time < timeout:
            return False

        return True
