import json
import logging
import uuid
from concurrent.futures import TimeoutError
from google.api_core import retry
from google.cloud import pubsub_v1

from octue import exceptions
from octue.mixins import CoolNameable
from octue.resources.communication.google_pub_sub import Subscription, Topic
from octue.resources.manifest import Manifest
from octue.utils.cloud.credentials import GCPCredentialsManager


logger = logging.getLogger(__name__)


OCTUE_NAMESPACE = "octue.services"
ANSWERS_NAMESPACE = "answers"


# Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)


class Service(CoolNameable):
    """A Twined service that can be used in two modes:
    * As a server accepting questions (input values and manifests), running them through its app, and responding to the
    requesting service with the results of the analysis.
    * As a requester of answers from another Service in the above mode.

    Services communicate entirely via Google Pub/Sub and can ask and/or respond to questions from any other Service that
    has a corresponding topic on Google Pub/Sub.
    """

    def __init__(self, backend, id=None, run_function=None):
        self.id = id
        self.backend = backend
        self.run_function = run_function
        self.publisher = pubsub_v1.PublisherClient(
            credentials=GCPCredentialsManager(backend.credentials_environment_variable).get_credentials(),
            batch_settings=BATCH_SETTINGS,
        )
        self.subscriber = pubsub_v1.SubscriberClient(
            credentials=GCPCredentialsManager(backend.credentials_environment_variable).get_credentials()
        )
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, timeout=None, delete_topic_and_subscription_on_exit=False):
        """Start the Service as a server, waiting to accept questions from any other Service using Google Pub/Sub on
        the same Google Cloud Platform project. Questions are responded to asynchronously."""
        topic = Topic(name=self.id, namespace=OCTUE_NAMESPACE, service=self)
        topic.create(allow_existing=True)

        subscription = Subscription(name=self.id, topic=topic, namespace=OCTUE_NAMESPACE, service=self)
        subscription.create(allow_existing=True)

        future = self.subscriber.subscribe(subscription=subscription.path, callback=self.answer)
        logger.debug("%r is waiting for questions.", self)

        with self.subscriber:
            try:
                future.result(timeout=timeout)
            except (TimeoutError, KeyboardInterrupt):
                future.cancel()

            if delete_topic_and_subscription_on_exit:
                topic.delete()
                subscription.delete()

    def answer(self, question):
        """Acknowledge and answer a question (i.e. receive the question (input values and/or manifest), run the
        Service's app to analyse it, and return the output values to the asker). Answers are published to a topic whose
        name is generated from the UUID sent with the question, and are in the format specified in the Service's Twine
        file.
        """
        logger.info("%r received a question.", self)
        data = json.loads(question.data.decode())
        question_uuid = question.attributes["question_uuid"]
        question.ack()

        topic = Topic(
            name=".".join((self.id, ANSWERS_NAMESPACE, question_uuid)), namespace=OCTUE_NAMESPACE, service=self
        )
        analysis = self.run_function(input_values=data["input_values"], input_manifest=data["input_manifest"])

        if analysis.output_manifest is None:
            serialised_output_manifest = None
        else:
            serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

        self.publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {"output_values": analysis.output_values, "output_manifest": serialised_output_manifest}
            ).encode(),
        )
        logger.info("%r responded on topic %r.", self, topic.path)

    def ask(self, service_id, input_values, input_manifest=None):
        """Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.
        """
        question_topic = Topic(name=service_id, namespace=OCTUE_NAMESPACE, service=self)
        if not question_topic.exists():
            raise exceptions.ServiceNotFound(f"Service with ID {service_id!r} cannot be found.")

        question_uuid = str(int(uuid.uuid4()))

        response_topic_and_subscription_name = ".".join((service_id, ANSWERS_NAMESPACE, question_uuid))
        response_topic = Topic(name=response_topic_and_subscription_name, namespace=OCTUE_NAMESPACE, service=self)
        response_topic.create(allow_existing=False)

        response_subscription = Subscription(
            name=response_topic_and_subscription_name,
            topic=response_topic,
            namespace=OCTUE_NAMESPACE,
            service=self,
        )
        response_subscription.create(allow_existing=False)

        if input_manifest is not None:
            input_manifest = input_manifest.serialise(to_string=True)

        future = self.publisher.publish(
            topic=question_topic.path,
            data=json.dumps({"input_values": input_values, "input_manifest": input_manifest}).encode(),
            question_uuid=question_uuid,
        )
        future.result()

        logger.debug("%r asked question to %r service. Question UUID is %r.", self, service_id, question_uuid)
        return response_subscription

    def wait_for_answer(self, subscription, timeout=20):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.
        """
        answer = self.subscriber.pull(
            request={"subscription": subscription.path, "max_messages": 1},
            retry=retry.Retry(deadline=timeout),
        ).received_messages[0]

        self.subscriber.acknowledge(request={"subscription": subscription.path, "ack_ids": [answer.ack_id]})
        logger.debug("%r received a response to question on topic %r", self, subscription.topic.path)

        subscription.delete()
        subscription.topic.delete()

        data = json.loads(answer.message.data.decode())

        if data["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(data["output_manifest"], from_string=True)

        return {"output_values": data["output_values"], "output_manifest": output_manifest}
