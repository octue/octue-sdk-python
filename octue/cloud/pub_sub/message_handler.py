import json
import logging
import time

from google.api_core import retry

import octue.exceptions
import twined.exceptions
from octue.resources.manifest import Manifest
from octue.utils.exceptions import create_exceptions_mapping


logger = logging.getLogger(__name__)

EXCEPTIONS_MAPPING = create_exceptions_mapping(
    globals()["__builtins__"], vars(twined.exceptions), vars(octue.exceptions)
)


class OrderedMessageHandler:
    """A handler for Google Pub/Sub messages that ensures messages are handled in the order they were sent.

    :param google.pubsub_v1.services.subscriber.client.SubscriberClient subscriber: a Google Pub/Sub subscriber
    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param str service_name: an arbitrary name to refer to the service subscribed to by (used for labelling its remote log messages)
    :param dict|None message_handlers: a mapping of message handler names to callables that handle each type of message
    :return None:
    """

    def __init__(
        self,
        subscriber,
        subscription,
        handle_monitor_message=None,
        service_name="REMOTE",
        message_handlers=None,
    ):
        self.subscriber = subscriber
        self.subscription = subscription
        self.handle_monitor_message = handle_monitor_message
        self.service_name = service_name

        self.received_delivery_acknowledgement = None
        self._start_time = time.perf_counter()
        self._waiting_messages = None
        self._previous_message_number = -1

        self._message_handlers = message_handlers or {
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

    def handle_messages(self, timeout=60, delivery_acknowledgement_timeout=30):
        """Pull messages and handle them in the order they were sent until a result is returned by a message handler,
        then return that result.

        :param float|None timeout: how long to wait for an answer before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :raise TimeoutError: if the timeout is exceeded before receiving the final message
        :return dict:
        """
        self.received_delivery_acknowledgement = False
        self._waiting_messages = {}
        self._previous_message_number = -1

        pull_timeout = None

        while True:

            if timeout is not None:
                run_time = time.perf_counter() - self._start_time

                if run_time > timeout:
                    raise TimeoutError(
                        f"No final answer received from topic {self.subscription.topic.path!r} after {timeout} seconds.",
                    )

                pull_timeout = timeout - run_time

            message = self._pull_message(
                timeout=pull_timeout,
                delivery_acknowledgement_timeout=delivery_acknowledgement_timeout,
            )

            self._waiting_messages[int(message["message_number"])] = message

            try:
                while self._waiting_messages:
                    message = self._waiting_messages.pop(self._previous_message_number + 1)
                    result = self._handle_message(message)

                    if result is not None:
                        return result

            except KeyError:
                pass

    def _pull_message(self, timeout, delivery_acknowledgement_timeout):
        """Pull a message from the subscription, raising a `TimeoutError` if the timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait in seconds for the message before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :raise octue.exceptions.QuestionNotDelivered: if a delivery acknowledgement is not received in time
        :return dict: message containing data
        """
        start_time = time.perf_counter()

        while True:
            attempt = 1

            while True:
                logger.debug("Pulling messages from Google Pub/Sub: attempt %d.", attempt)

                pull_response = self.subscriber.pull(
                    request={"subscription": self.subscription.path, "max_messages": 1},
                    retry=retry.Retry(),
                )

                try:
                    answer = pull_response.received_messages[0]
                    break

                except IndexError:
                    logger.debug("Google Pub/Sub pull response timed out early.")
                    attempt += 1

                    run_time = time.perf_counter() - start_time

                    if timeout is not None and run_time > timeout:
                        raise TimeoutError(
                            f"No message received from topic {self.subscription.topic.path!r} after {timeout} seconds.",
                        )

                    if not self.received_delivery_acknowledgement:
                        if run_time > delivery_acknowledgement_timeout:
                            raise octue.exceptions.QuestionNotDelivered(
                                f"No delivery acknowledgement received for topic {self.subscription.topic.path!r} "
                                f"after {delivery_acknowledgement_timeout} seconds."
                            )

            self.subscriber.acknowledge(request={"subscription": self.subscription.path, "ack_ids": [answer.ack_id]})

            logger.debug(
                "%r received a message related to question %r.",
                self.subscription.topic.service,
                self.subscription.topic.path.split(".")[-1],
            )

            return json.loads(answer.message.data.decode())

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self._previous_message_number += 1

        try:
            return self._message_handlers[message["type"]](message)
        except KeyError:
            logger.warning("Received a message of unknown type %r.", message["type"])

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        self.received_delivery_acknowledgement = True
        logger.info("%r's question was delivered at %s.", self.subscription.topic.service, message["delivery_time"])

    def _handle_monitor_message(self, message):
        """Send a monitor message to the handler if one has been provided.

        :param dict message:
        :return None:
        """
        logger.debug("%r received a monitor message.", self.subscription.topic.service)

        if self.handle_monitor_message is not None:
            self.handle_monitor_message(json.loads(message["data"]))

    def _handle_log_message(self, message):
        """Deserialise the message into a log record and pass it to the local log handlers, adding [<service-name>] to
        the start of the log message.

        :param dict message:
        :return None:
        """
        record = logging.makeLogRecord(message["log_record"])
        record.msg = f"[{self.service_name} | analysis-{message['analysis_id']}] {record.msg}"
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
                f"The following traceback was captured from the remote service {self.service_name!r}:",
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
        logger.info("Received an answer to question %r.", self.subscription.topic.path.split(".")[-1])

        if message["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(message["output_manifest"], from_string=True)

        return {"output_values": message["output_values"], "output_manifest": output_manifest}
