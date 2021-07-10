import base64
import concurrent.futures
import json
import logging
import sys
import time
import traceback as tb
import uuid
from google.cloud import pubsub_v1

import octue.exceptions
import twined.exceptions
from octue.cloud.credentials import GCPCredentialsManager
from octue.cloud.pub_sub import Subscription, Topic, create_custom_retry
from octue.cloud.pub_sub.logging import GooglePubSubHandler
from octue.mixins import CoolNameable
from octue.resources.manifest import Manifest
from octue.utils.encoders import OctueJSONEncoder
from octue.utils.exceptions import create_exceptions_mapping
from octue.utils.objects import get_nested_attribute


logger = logging.getLogger(__name__)

OCTUE_NAMESPACE = "octue.services"
ANSWERS_NAMESPACE = "answers"

# Switch message batching off by setting max_messages to 1. This minimises latency and is recommended for
# microservices publishing single messages in a request-response sequence.
BATCH_SETTINGS = pubsub_v1.types.BatchSettings(max_bytes=10 * 1000 * 1000, max_latency=0.01, max_messages=1)

EXCEPTIONS_MAPPING = create_exceptions_mapping(
    globals()["__builtins__"], vars(twined.exceptions), vars(octue.exceptions)
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
        the same Google Cloud Platform project. Questions are responded to asynchronously.

        :param float|None timeout: time in seconds after which to shut down the service
        :param bool delete_topic_and_subscription_on_exit: if `True`, delete the service's topic and subscription on exit
        :return None:
        """
        topic = Topic(name=self.id, namespace=OCTUE_NAMESPACE, service=self)
        topic.create(allow_existing=True)

        subscription = Subscription(
            name=self.id, topic=topic, namespace=OCTUE_NAMESPACE, service=self, expiration_time=None
        )
        subscription.create(allow_existing=True)

        future = self.subscriber.subscribe(subscription=subscription.path, callback=self.answer)
        logger.debug("%r is waiting for questions.", self)

        with self.subscriber:
            try:
                future.result(timeout=timeout)
            except (TimeoutError, concurrent.futures.TimeoutError, KeyboardInterrupt):
                future.cancel()

            if delete_topic_and_subscription_on_exit:
                topic.delete()
                subscription.delete()

    def answer(self, question, timeout=30):
        """Answer a question (i.e. run the Service's app to analyse the given data, and return the output values to the
        asker). Answers are published to a topic whose name is generated from the UUID sent with the question, and are
        in the format specified in the Service's Twine file.

        :param dict|Message question:
        :param float timeout:
        :raise Exception: if any exception arises during running analysis and sending its results
        :return None:
        """
        data, question_uuid, forward_logs = self.parse_question(question)

        topic = Topic(
            name=".".join((self.id, ANSWERS_NAMESPACE, question_uuid)), namespace=OCTUE_NAMESPACE, service=self
        )

        if forward_logs:
            analysis_log_handler = GooglePubSubHandler(publisher=self.publisher, topic=topic)
        else:
            analysis_log_handler = None

        try:
            analysis = self.run_function(
                analysis_id=question_uuid,
                input_values=data["input_values"],
                input_manifest=data["input_manifest"],
                analysis_log_handler=analysis_log_handler,
            )

            if analysis.output_manifest is None:
                serialised_output_manifest = None
            else:
                serialised_output_manifest = analysis.output_manifest.serialise(to_string=True)

            self.publisher.publish(
                topic=topic.path,
                data=json.dumps(
                    {
                        "type": "result",
                        "output_values": analysis.output_values,
                        "output_manifest": serialised_output_manifest,
                        "message_number": topic.messages_published,
                    },
                    cls=OctueJSONEncoder,
                ).encode(),
                retry=create_custom_retry(timeout),
            )
            topic.messages_published += 1
            logger.info("%r responded to question %r.", self, question_uuid)

        except BaseException as error:  # noqa
            self._send_exception_to_asker(topic, timeout)
            raise error

    def parse_question(self, question):
        """Parse a question in the Google Cloud Pub/Sub or Google Cloud Run format.

        :param dict|Message question:
        :return (dict, str, bool):
        """
        try:
            # Parse Google Cloud Pub/Sub question format.
            data = json.loads(question.data.decode())
            question.ack()
            logger.info("%r received a question.", self)
        except Exception:
            # Parse Google Cloud Run question format.
            data = json.loads(base64.b64decode(question["data"]).decode("utf-8").strip())

        question_uuid = get_nested_attribute(question, "attributes.question_uuid")
        forward_logs = bool(int(get_nested_attribute(question, "attributes.forward_logs")))
        return data, question_uuid, forward_logs

    def ask(self, service_id, input_values, input_manifest=None, subscribe_to_logs=True):
        """Ask a serving Service a question (i.e. send it input values for it to run its app on). The input values must
        be in the format specified by the serving Service's Twine file. A single-use topic and subscription are created
        before sending the question to the serving Service - the topic is the expected publishing place for the answer
        from the serving Service when it comes, and the subscription is set up to subscribe to this.

        :param str service_id: the UUID of the service to ask the question to
        :param any input_values: the input values of the question
        :param octue.resources.manifest.Manifest|None input_manifest: the input manifest of the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the remote service and handle them with the local log handlers
        :return (octue.cloud.pub_sub.subscription.Subscription, str): the response subscription and question UUID
        """
        if (input_manifest is not None) and (not input_manifest.all_datasets_are_in_cloud):
            raise octue.exceptions.FileLocationError(
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
            forward_logs=str(int(subscribe_to_logs)),
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
        message_handler = OrderedMessageHandler(message_puller=self._pull_message, subscription=subscription)

        with self.subscriber:
            try:
                return message_handler.handle_messages(timeout=timeout)

            finally:
                subscription.delete()
                subscription.topic.delete()

    def _pull_message(self, subscription, timeout):
        """Pull a message from the subscription, raising a `TimeoutError` if the timeout is exceeded before succeeding.

        :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription the message is expected on
        :param float timeout: how long to wait in seconds for the message before raising a TimeoutError
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :return dict: message containing data
        """
        start_time = time.perf_counter()

        while True:
            no_message = True
            attempt = 1

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
                            f"No message received from topic {subscription.topic.path!r} after {timeout} seconds.",
                        )

                    continue

            self.subscriber.acknowledge(request={"subscription": subscription.path, "ack_ids": [answer.ack_id]})
            logger.debug("%r received a message related to question %r.", self, subscription.topic.path.split(".")[-1])
            return json.loads(answer.message.data.decode())

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
                    "type": "exception",
                    "exception_type": type(exception).__name__,
                    "exception_message": exception_message,
                    "traceback": traceback,
                    "message_number": topic.messages_published,
                }
            ).encode(),
            retry=create_custom_retry(timeout),
        )

        topic.messages_published += 1


class OrderedMessageHandler:
    """A handler for Google Pub/Sub messages that ensures messages are handled in the order they were sent.

    :param callable message_puller: function that pulls a message from the subscription
    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :return None:
    """

    def __init__(self, message_puller, subscription, message_handlers=None):
        self.message_puller = message_puller
        self.subscription = subscription
        self._waiting_messages = {}
        self._previous_message_number = -1

        self._message_handlers = message_handlers or {
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

    def handle_messages(self, timeout=30):
        """Pull messages and handle them in the order they were sent until a result is returned by a message handler,
        then return that result.

        :param float timeout: how long to wait for an answer before raising a `TimeoutError`
        :raise TimeoutError: if the timeout is exceeded before receiving the final message
        :return dict:
        """
        start_time = time.perf_counter()

        while True:
            run_time = time.perf_counter() - start_time

            if run_time > timeout:
                raise TimeoutError(
                    f"No final answer received from topic {self.subscription.topic.path!r} after {timeout} seconds.",
                )

            pull_timeout = timeout - run_time
            message = self.message_puller(self.subscription, timeout=pull_timeout)
            self._waiting_messages[message["message_number"]] = message

            try:
                while self._waiting_messages:
                    message = self._waiting_messages.pop(self._previous_message_number + 1)
                    result = self._handle_message(message)

                    if result is not None:
                        return result

            except KeyError:
                pass

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self._previous_message_number += 1

        try:
            return self._message_handlers[message["type"]](message)
        except KeyError:
            logger.warning("%r received a message of unknown type %r.", self.subscription.service, message["type"])

    def _handle_log_message(self, message):
        """Deserialise the message into a log record and pass it to the local log handlers, adding `[REMOTE] to the
        start of the log message.

        :param dict message:
        :return None:
        """
        record = logging.makeLogRecord(message["log_record"])
        record.msg = f"[REMOTE] {record.msg}"
        logger.handle(record)

    def _handle_exception(self, message):
        """Raise the exception from the responding service that is serialised in `data`.

        :param dict message:
        :raise Exception:
        :return None:
        """
        exception_message = "\n\n".join(
            (
                message["exception_message"],
                "The following traceback was captured from the remote service:",
                "".join(message["traceback"]),
            )
        )

        try:
            raise EXCEPTIONS_MAPPING[message["exception_type"]](exception_message)

        # Allow unknown exception types to still be raised.
        except KeyError:
            raise type(message["exception_type"], (Exception,), {})(exception_message)

    def _handle_result(self, message):
        """Convert the result to the correct form, deserialising the output manifest if it is present in the message.

        :param dict message:
        :return dict:
        """
        logger.info(
            "%r received an answer to question %r.",
            self.subscription.service,
            self.subscription.topic.path.split(".")[-1],
        )

        if message["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(message["output_manifest"], from_string=True)

        return {"output_values": message["output_values"], "output_manifest": output_manifest}
