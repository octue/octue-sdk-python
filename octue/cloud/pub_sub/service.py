import json
import logging
import sys
import time
import traceback as tb
import uuid
from concurrent.futures import TimeoutError
import google.api_core
import google.api_core.exceptions
from google.api_core import retry
from google.cloud import pubsub_v1

import octue.exceptions
import twined.exceptions
from octue.cloud.credentials import GCPCredentialsManager
from octue.cloud.pub_sub import Subscription, Topic
from octue.exceptions import FileLocationError
from octue.mixins import CoolNameable
from octue.resources.manifest import Manifest
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.exceptions import create_exceptions_mapping


logger = logging.getLogger(__name__)

OCTUE_NAMESPACE = "octue.services"
ANSWERS_NAMESPACE = "answers"

# Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)

EXCEPTIONS_MAPPING = create_exceptions_mapping(
    globals()["__builtins__"], vars(twined.exceptions), vars(octue.exceptions)
)


def create_custom_retry(timeout):
    """Create a custom `Retry` object specifying that the given Google Cloud request should retry for the given amount
    of time for the given exceptions.

    :param float timeout:
    :return google.api_core.retry.Retry:
    """
    return retry.Retry(
        maximum=timeout / 4,
        deadline=timeout,
        predicate=google.api_core.retry.if_exception_type(
            google.api_core.exceptions.NotFound,
            google.api_core.exceptions.Aborted,
            google.api_core.exceptions.DeadlineExceeded,
            google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ResourceExhausted,
            google.api_core.exceptions.ServiceUnavailable,
            google.api_core.exceptions.Unknown,
            google.api_core.exceptions.Cancelled,
        ),
    )


class Service(CoolNameable):
    """A Twined service that can be used in two modes:
    * As a server accepting questions (input values and manifests), running them through its app, and responding to the
    requesting service with the results of the analysis.
    * As a requester of answers from another Service in the above mode.

    Services communicate entirely via Google Pub/Sub and can ask and/or respond to questions from any other Service that
    has a corresponding topic on Google Pub/Sub.

    :param octue.resources.service_backends.ServiceBackend backend:
    :param str|None service_id:
    :param callable|None run_function:
    :return None:
    """

    def __init__(self, backend, service_id=None, run_function=None):
        if service_id is None:
            self.id = str(uuid.uuid4())
        elif not service_id:
            raise ValueError(f"service_id should be None or a non-falsey value; received {service_id!r} instead.")
        else:
            self.id = service_id

        self.backend = backend
        self.run_function = run_function

        credentials = GCPCredentialsManager(backend.credentials_environment_variable).get_credentials()
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials, batch_settings=BATCH_SETTINGS)
        self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        super().__init__()

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def serve(self, timeout=None, delete_topic_and_subscription_on_exit=False):
        """Start the Service as a server, waiting to accept questions from any other Service using Google Pub/Sub on
        the same Google Cloud Platform project. Questions are responded to asynchronously."""
        topic = Topic(name=self.id, namespace=OCTUE_NAMESPACE, service=self)
        topic.create(allow_existing=True)

        subscription = Subscription(
            name=self.id, topic=topic, namespace=OCTUE_NAMESPACE, service=self, expiration_time=None
        )
        subscription.create(allow_existing=True)

        future = self.subscriber.subscribe(subscription=subscription.path, callback=self.receive_question_then_answer)
        logger.debug("%r is waiting for questions.", self)

        with self.subscriber:
            try:
                future.result(timeout=timeout)
            except (TimeoutError, KeyboardInterrupt):
                future.cancel()

            if delete_topic_and_subscription_on_exit:
                topic.delete()
                subscription.delete()

    def receive_question_then_answer(self, question):
        """Receive a question, acknowledge it, then answer it."""
        logger.info("%r received a question.", self)
        data = json.loads(question.data.decode())
        question_uuid = question.attributes["question_uuid"]
        question.ack()
        self.answer(data, question_uuid)

    def answer(self, data, question_uuid, timeout=30):
        """Answer a question (i.e. run the Service's app to analyse the given data, and return the output values to the
        asker). Answers are published to a topic whose name is generated from the UUID sent with the question, and are
        in the format specified in the Service's Twine file.

        :param dict data:
        :param str question_uuid:
        :param float timeout:
        :raise Exception: if any exception arises during running analysis and sending its results
        :return None:
        """
        topic = Topic(
            name=".".join((self.id, ANSWERS_NAMESPACE, question_uuid)), namespace=OCTUE_NAMESPACE, service=self
        )

        try:
            analysis = self.run_function(
                input_values=data["input_values"], input_manifest=data["input_manifest"], analysis_id=question_uuid
            )

            if analysis.output_manifest is None:
                serialised_output_manifest = None
            else:
                serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

            self.publisher.publish(
                topic=topic.path,
                data=json.dumps(
                    {"output_values": analysis.output_values, "output_manifest": serialised_output_manifest},
                    cls=OctueJSONEncoder,
                ).encode(),
                retry=create_custom_retry(timeout),
            )
            logger.info("%r responded to question %r.", self, question_uuid)

        except BaseException as error:  # noqa
            self._send_exception_to_asker(topic, timeout)
            raise error

    def ask(self, service_id, input_values, input_manifest=None):
        """Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.
        """
        if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
            raise FileLocationError(
                "All datasets of the input manifest and all files of the datasets must be uploaded to the cloud before "
                "asking a service to perform an analysis upon them. The manifest must then be updated with the new "
                "cloud locations."
            )

        question_topic = Topic(name=service_id, namespace=OCTUE_NAMESPACE, service=self)
        if not question_topic.exists():
            raise octue.exceptions.ServiceNotFound(f"Service with ID {service_id!r} cannot be found.")

        question_uuid = str(uuid.uuid4())

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

        logger.info("%r asked a question %r to service %r.", self, question_uuid, service_id)
        return response_subscription, question_uuid

    def wait_for_answer(self, subscription, timeout=30):
        """Wait for an answer to a question on the given subscription, deleting the subscription and its topic once
        the answer is received.

        :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription for the question's answer
        :param float timeout: how long to wait for an answer before raising a TimeoutError
        :raise TimeoutError: if the timeout is exceeded
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        start_time = time.perf_counter()
        no_message = True
        attempt = 1

        with self.subscriber:

            try:
                while no_message:
                    logger.debug("Pulling messages from Google Pub/Sub: attempt %d.", attempt)

                    pull_response = self.subscriber.pull(
                        request={"subscription": subscription.path, "max_messages": 1},
                        retry=create_custom_retry(timeout),
                    )

                    try:
                        answer = pull_response.received_messages[0]
                        no_message = False

                    except IndexError:
                        logger.debug("Google Pub/Sub pull response timed out early.")
                        attempt += 1

                        if (time.perf_counter() - start_time) > timeout:
                            raise TimeoutError(
                                f"No answer received from topic {subscription.topic.path!r} after {timeout} seconds.",
                            )

            finally:
                try:
                    self.subscriber.acknowledge(request={"subscription": subscription.path, "ack_ids": [answer.ack_id]})
                    logger.info("%r received a response to question %r.", self, subscription.topic.path.split(".")[-1])
                except UnboundLocalError:
                    pass

                subscription.delete()
                subscription.topic.delete()

        data = json.loads(answer.message.data.decode())

        if "exception_type" in data:
            self._raise_exception_from_responder(data)

        if data["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(data["output_manifest"], from_string=True)

        return {"output_values": data["output_values"], "output_manifest": output_manifest}

    def _send_exception_to_asker(self, topic, timeout):
        """Serialise and send the exception being handled to the asker.

        :param octue.cloud.pub_sub.topic.Topic topic:
        :param float timeout:
        :return None:
        """
        exception_info = sys.exc_info()
        exception = exception_info[1]
        exception_message = f"Error in {self!r}: " + exception.args[0]
        traceback = tb.format_list(tb.extract_tb(exception_info[2]))

        self.publisher.publish(
            topic=topic.path,
            data=json.dumps(
                {
                    "exception_type": type(exception).__name__,
                    "exception_message": exception_message,
                    "traceback": traceback,
                }
            ).encode(),
            retry=create_custom_retry(timeout),
        )

    def _raise_exception_from_responder(self, data):
        """Raise the exception from the responding service that is serialised in `data`.

        :param dict data:
        :raise Exception:
        :return None:
        """
        message = "\n\n".join(
            (
                data["exception_message"],
                "The following traceback was captured from the remote service:",
                "".join(data["traceback"]),
            )
        )

        try:
            raise EXCEPTIONS_MAPPING[data["exception_type"]](message)

        # Allow unknown exception types to still be raised.
        except KeyError:
            raise type(data["exception_type"], (Exception,), {})(message)
