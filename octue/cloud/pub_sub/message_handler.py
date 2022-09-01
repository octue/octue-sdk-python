import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

from google.api_core import retry

from octue.cloud import EXCEPTIONS_MAPPING
from octue.definitions import GOOGLE_COMPUTE_PROVIDERS
from octue.exceptions import QuestionNotDelivered
from octue.log_handlers import COLOUR_PALETTE
from octue.resources.manifest import Manifest
from octue.utils.threads import RepeatingTimer


if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") in GOOGLE_COMPUTE_PROVIDERS:
    # Google Cloud logs don't support colour currently - provide a no-operation function.
    colourise = lambda string, text_colour=None, background_colour=None: string
else:
    from octue.utils.colour import colourise


logger = logging.getLogger(__name__)


class OrderedMessageHandler:
    """A handler for Google Pub/Sub messages that ensures messages are handled in the order they were sent.

    :param google.pubsub_v1.services.subscriber.client.SubscriberClient subscriber: a Google Pub/Sub subscriber
    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param str|None record_messages_to: if given a path to a JSON file, received messages are saved to it
    :param str service_name: an arbitrary name to refer to the service subscribed to by (used for labelling its remote log messages)
    :param dict|None message_handlers: a mapping of message type names to callables that handle each type of message. The handlers should not mutate the messages.
    :return None:
    """

    def __init__(
        self,
        subscriber,
        subscription,
        handle_monitor_message=None,
        record_messages_to=None,
        service_name="REMOTE",
        message_handlers=None,
    ):
        self.subscriber = subscriber
        self.subscription = subscription
        self.handle_monitor_message = handle_monitor_message
        self.record_messages_to = record_messages_to
        self.service_name = service_name

        self.received_delivery_acknowledgement = None
        self._child_sdk_version = None
        self._heartbeat_checker = None
        self._last_heartbeat = None
        self._alive = True
        self._start_time = time.perf_counter()
        self._waiting_messages = None
        self._previous_message_number = -1

        self._message_handlers = message_handlers or {
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "heartbeat": self._handle_heartbeat,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

        self._log_message_colours = [COLOUR_PALETTE[1], *COLOUR_PALETTE[3:]]

    @property
    def _time_since_last_heartbeat(self):
        """Get the time period since the last heartbeat was received.

        :return datetime.timedelta|None:
        """
        if not self._last_heartbeat:
            return None

        return datetime.now() - self._last_heartbeat

    def handle_messages(self, timeout=60, delivery_acknowledgement_timeout=120, maximum_heartbeat_interval=300):
        """Pull messages and handle them in the order they were sent until a result is returned by a message handler,
        then return that result.

        :param float|None timeout: how long to wait for an answer before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :param int|float maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded before receiving the final message
        :return dict:
        """
        self.received_delivery_acknowledgement = False
        self._waiting_messages = {}
        self._previous_message_number = -1

        recorded_messages = []
        pull_timeout = None

        self._heartbeat_checker = RepeatingTimer(
            interval=maximum_heartbeat_interval,
            function=self._monitor_heartbeat,
            kwargs={"maximum_heartbeat_interval": maximum_heartbeat_interval},
        )

        self._heartbeat_checker.daemon = True
        self._heartbeat_checker.start()

        while self._alive:

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

                    if self.record_messages_to:
                        recorded_messages.append(message)

                    result = self._handle_message(message)

                    if result is not None:
                        self._heartbeat_checker.cancel()
                        return result

            except KeyError:
                pass

            finally:
                self._heartbeat_checker.cancel()

                if self.record_messages_to:
                    directory_name = os.path.dirname(self.record_messages_to)

                    if not os.path.exists(directory_name):
                        os.makedirs(directory_name)

                    with open(self.record_messages_to, "w") as f:
                        json.dump(recorded_messages, f)

        raise TimeoutError(
            f"No heartbeat has been received within the maximum allowed interval of {maximum_heartbeat_interval}s."
        )

    def _monitor_heartbeat(self, maximum_heartbeat_interval):
        """Change the alive status to `False` and cancel the heartbeat checker if a heartbeat hasn't been received
        within the maximum allowed time interval measured from the moment of calling.

        :param float|int maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats without raising an error
        :return None:
        """
        maximum_heartbeat_interval = timedelta(seconds=maximum_heartbeat_interval)

        if self._last_heartbeat and self._time_since_last_heartbeat <= maximum_heartbeat_interval:
            self._alive = True
            return

        self._alive = False
        self._heartbeat_checker.cancel()

    def _pull_message(self, timeout, delivery_acknowledgement_timeout):
        """Pull a message from the subscription, raising a `TimeoutError` if the timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait in seconds for the message before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :raise octue.exceptions.QuestionNotDelivered: if a delivery acknowledgement is not received in time
        :return dict: message containing data
        """
        start_time = time.perf_counter()
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
                        raise QuestionNotDelivered(
                            f"No delivery acknowledgement received for topic {self.subscription.topic.path!r} "
                            f"after {delivery_acknowledgement_timeout} seconds."
                        )

        self.subscriber.acknowledge(request={"subscription": self.subscription.path, "ack_ids": [answer.ack_id]})

        logger.debug(
            "%r received a message related to question %r.",
            self.subscription.topic.service,
            self.subscription.topic.path.split(".")[-1],
        )

        # Get the child's Octue SDK version from the first message.
        if not self._child_sdk_version:
            self._child_sdk_version = answer.message.attributes.get("octue_sdk_version")

            # If the child hasn't provided its Octue SDK version, it's too old to send heartbeats - so, cancel the
            # heartbeat checker to maintain compatibility.
            if not self._child_sdk_version:
                self._heartbeat_checker.cancel()

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
            logger.warning(
                "%r received a message of unknown type %r.",
                self.subscription.topic.service,
                message["type"],
            )

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        self.received_delivery_acknowledgement = True
        logger.info("%r's question was delivered at %s.", self.subscription.topic.service, message["delivery_time"])

    def _handle_heartbeat(self, message):
        """Record the time the heartbeat was received.

        :param dict message:
        :return None:
        """
        self._last_heartbeat = datetime.now()
        logger.info("Heartbeat received from service %r.", self.service_name)

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

        # Add information about the immediate child sending the message and colour it with the first colour in the
        # colour palette.
        immediate_child_analysis_section = colourise(
            f"[{self.service_name} | analysis-{message['analysis_id']}]",
            text_colour=self._log_message_colours[0],
        )

        # Colour any analysis sections from children of the immediate child with the rest of the colour palette and
        # colour the message from the furthest child white.
        subchild_analysis_sections = [section.strip("[") for section in re.split("] ", record.msg)]
        final_message = subchild_analysis_sections.pop(-1)

        for i in range(len(subchild_analysis_sections)):
            subchild_analysis_sections[i] = colourise(
                "[" + subchild_analysis_sections[i] + "]",
                text_colour=self._log_message_colours[1:][i % len(self._log_message_colours[1:])],
            )

        record.msg = " ".join([immediate_child_analysis_section, *subchild_analysis_sections, final_message])
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
            exception_type = EXCEPTIONS_MAPPING[message["exception_type"]]

        # Allow unknown exception types to still be raised.
        except KeyError:
            exception_type = type(message["exception_type"], (Exception,), {})

        raise exception_type(exception_message)

    def _handle_result(self, message):
        """Convert the result to the correct form, deserialising the output manifest if it is present in the message.

        :param dict message:
        :return dict:
        """
        logger.info(
            "%r received an answer to question %r.",
            self.subscription.topic.service,
            self.subscription.topic.path.split(".")[-1],
        )

        if message["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(message["output_manifest"], from_string=True)

        return {"output_values": message["output_values"], "output_manifest": output_manifest}
