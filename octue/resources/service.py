import json
import logging
import uuid
from concurrent.futures import TimeoutError
import google.api_core.exceptions
from google.api_core import retry
from google.cloud import pubsub_v1

from octue import exceptions
from octue.mixins import CoolNameable
from octue.resources.manifest import Manifest


logger = logging.getLogger(__name__)


OCTUE_NAMESPACE = "octue.services"
ANSWERS_NAMESPACE = "answers"


# Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)


class Topic:
    """ A candidate topic to use with Google Pub/Sub. The topic represented by an instance of this class does not
    necessarily already exist on the Google Pub/Sub servers.
    """

    def __init__(self, name, service):
        self.name = name
        self.service = service
        self.path = self.service._publisher.topic_path(service.backend.project_name, f"{OCTUE_NAMESPACE}.{self.name}")

    def create(self, allow_existing=False):
        """ Create a Google Pub/Sub topic that can be published to. """
        if not allow_existing:
            self.service._publisher.create_topic(name=self.path)
            self._log_creation()
            return

        try:
            self.service._publisher.create_topic(name=self.path)
        except google.api_core.exceptions.AlreadyExists:
            pass
        self._log_creation()

    def delete(self):
        """ Delete the topic from Google Pub/Sub. """
        self.service._publisher.delete_topic(topic=self.path)
        logger.debug("%r deleted topic %r.", self.service, self.path)

    def exists(self):
        """ Check if the topic exists on the Google Pub/Sub servers. """
        try:
            self.service._publisher.get_topic(topic=self.path)
        except google.api_core.exceptions.NotFound:
            return False
        return True

    def _log_creation(self):
        """ Log the creation of the topic. """
        logger.debug("%r created topic %r.", self.service, self.path)


class Subscription:
    """ A candidate subscription to use with Google Pub/Sub. The subscription represented by an instance of this class
    does not necessarily already exist on the Google Pub/Sub servers.
    """

    def __init__(self, name, topic, service):
        self.name = name
        self.topic = topic
        self.service = service
        self.path = self.service._subscriber.subscription_path(
            self.service.backend.project_name, f"{OCTUE_NAMESPACE}.{self.name}"
        )

    def create(self, allow_existing=False):
        """ Create a Google Pub/Sub subscription that can be subscribed to. """
        if not allow_existing:
            self.service._subscriber.create_subscription(topic=self.topic.path, name=self.path)
            self._log_creation()
            return

        try:
            self.service._subscriber.create_subscription(topic=self.topic.path, name=self.path)
        except google.api_core.exceptions.AlreadyExists:
            pass
        self._log_creation()

    def delete(self):
        """ Delete the subscription from Google Pub/Sub. """
        self.service._subscriber.delete_subscription(subscription=self.path)
        logger.debug("%r deleted subscription %r.", self.service, self.path)

    def _log_creation(self):
        """ Log the creation of the subscription. """
        logger.debug("%r created subscription %r.", self.service, self.path)


class Service(CoolNameable):
    """ A Twined service that can be used in two modes:
    * As a server accepting questions (input values and manifests), running them through its app, and responding to the
    requesting service with the results of the analysis.
    * As a requester of answers from another Service in the above mode.

    Services communicate entirely via Google Pub/Sub and can ask and/or respond to questions from any other Service that
    has a corresponding topic on Google Pub/Sub.
    """

    def __init__(self, name, backend, id=None, run_function=None):
        self.name = name
        self.id = id
        self.backend = backend
        self.run_function = run_function
        self._publisher = pubsub_v1.PublisherClient.from_service_account_file(
            filename=backend.credentials_filename, batch_settings=BATCH_SETTINGS
        )
        self._subscriber = pubsub_v1.SubscriberClient.from_service_account_file(filename=backend.credentials_filename)
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.cool_name!r})>"

    def serve(self, timeout=None):
        """ Start the Service as a server, waiting to accept questions from any other Service using Google Pub/Sub on
        the same Google Cloud Platform project. Questions are responded to asynchronously."""
        topic = Topic(name=self.id, service=self)
        topic.create(allow_existing=True)

        subscription = Subscription(name=self.id, topic=topic, service=self)
        subscription.create(allow_existing=True)

        future = self._subscriber.subscribe(subscription=subscription.path, callback=self.answer)
        logger.debug("%r is waiting for questions.", self)

        with self._subscriber:
            try:
                future.result(timeout=timeout)
            except TimeoutError:
                future.cancel()

    def answer(self, question):
        """ Acknowledge and answer a question (i.e. receive the question (input values and/or manifest), run the
        Service's app to analyse it, and return the output values to the asker). Answers are published to a topic whose
        name is generated from the UUID sent with the question, and are in the format specified in the Service's Twine
        file.
        """
        logger.info("%r received a question.", self)
        data = json.loads(question.data.decode())
        question_uuid = question.attributes["question_uuid"]
        question.ack()

        topic = Topic(name=".".join((self.id, ANSWERS_NAMESPACE, question_uuid)), service=self)
        analysis = self.run_function(input_values=data["input_values"], input_manifest=data["input_manifest"])

        if analysis.output_manifest is None:
            serialised_output_manifest = None
        else:
            serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

        self._publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {"output_values": analysis.output_values, "output_manifest": serialised_output_manifest}
            ).encode(),
        )
        logger.info("%r responded on topic %r.", self, topic.path)

    def ask(self, service_id, input_values, input_manifest=None):
        """ Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.
        """
        question_topic = Topic(name=service_id, service=self)
        if not question_topic.exists():
            raise exceptions.ServiceNotFound(f"Service with ID {service_id!r} cannot be found.")

        question_uuid = str(int(uuid.uuid4()))

        response_topic_and_subscription_name = ".".join((service_id, ANSWERS_NAMESPACE, question_uuid))
        response_topic = Topic(name=response_topic_and_subscription_name, service=self)
        response_topic.create(allow_existing=False)

        response_subscription = Subscription(
            name=response_topic_and_subscription_name, topic=response_topic, service=self,
        )
        response_subscription.create(allow_existing=False)

        if input_manifest is not None:
            input_manifest = input_manifest.serialise(to_string=True)

        future = self._publisher.publish(
            topic=question_topic.path,
            data=json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode(),
            question_uuid=question_uuid,
        )
        future.result()

        logger.debug("%r asked question to %r service. Question UUID is %r.", self, service_id, question_uuid)
        return response_subscription

    def wait_for_answer(self, subscription, timeout=20):
        """ Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.
        """
        answer = self._subscriber.pull(
            request={"subscription": subscription.path, "max_messages": 1}, retry=retry.Retry(deadline=timeout),
        ).received_messages[0]

        self._subscriber.acknowledge(request={"subscription": subscription.path, "ack_ids": [answer.ack_id]})
        logger.debug("%r received a response to question on topic %r", self, subscription.topic.path)

        subscription.delete()
        subscription.topic.delete()

        data = json.loads(answer.message.data.decode())

        if data["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(data["output_manifest"], from_string=True)

        return {"output_values": data["output_values"], "output_manifest": output_manifest}
