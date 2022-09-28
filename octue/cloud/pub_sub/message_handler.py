import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import pkg_resources
from google.api_core import retry
from google.cloud.pubsub_v1 import SubscriberClient

from octue.cloud import EXCEPTIONS_MAPPING
from octue.compatibility import warn_if_incompatible
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

    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param octue.cloud.pub_sub.service.Service receiving_service: the service that's receiving the messages
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param str|None record_messages_to: if given a path to a JSON file, received messages are saved to it
    :param str service_name: an arbitrary name to refer to the service subscribed to by (used for labelling its remote log messages)
    :param dict|None message_handlers: a mapping of message type names to callables that handle each type of message. The handlers should not mutate the messages.
    :return None:
    """

    def __init__(
        self,
        subscription,
        receiving_service,
        handle_monitor_message=None,
        record_messages_to=None,
        service_name="REMOTE",
        message_handlers=None,
    ):
        self.subscription = subscription
        self.receiving_service = receiving_service
        self.handle_monitor_message = handle_monitor_message
        self.record_messages_to = record_messages_to
        self.service_name = service_name

        self.received_delivery_acknowledgement = None
        self._subscriber = SubscriberClient()
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
        :return dict: the first result returned by a message handler
        """
        self.received_delivery_acknowledgement = False
        self._waiting_messages = {}
        self._previous_message_number = -1

        recorded_messages = []

        self._heartbeat_checker = RepeatingTimer(
            interval=maximum_heartbeat_interval,
            function=self._monitor_heartbeat,
            kwargs={"maximum_heartbeat_interval": maximum_heartbeat_interval},
        )

        try:
            self._heartbeat_checker.daemon = True
            self._heartbeat_checker.start()

            while self._alive:
                pull_timeout = self._check_timeout_and_get_pull_timeout(timeout)

                self._pull_and_enqueue_message(
                    timeout=pull_timeout,
                    delivery_acknowledgement_timeout=delivery_acknowledgement_timeout,
                )

                result = self._attempt_to_handle_queued_messages(recorded_messages=recorded_messages)

                if result is not None:
                    return result

        finally:
            self._heartbeat_checker.cancel()
            self._subscriber.close()

            if self.record_messages_to:
                self._save_messages(recorded_messages)

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

    def _check_timeout_and_get_pull_timeout(self, timeout):
        """Check if the timeout has been exceeded and, if it hasn't, return the timeout for the next message pull. If
        the timeout has been exceeded, raise an error.

        :param int|float|None timeout: the timeout for handling all messages
        :raise TimeoutError: if the timeout has been exceeded
        :return int|float: the timeout for the next message pull in seconds
        """
        if timeout is None:
            return None

        run_time = time.perf_counter() - self._start_time

        if run_time > timeout:
            raise TimeoutError(
                f"No final answer received from topic {self.subscription.topic.path!r} after {timeout} seconds."
            )

        return timeout - run_time

    def _pull_and_enqueue_message(self, timeout, delivery_acknowledgement_timeout):
        """Pull a message from the subscription and enqueue it in `self._waiting_messages`, raising a `TimeoutError` if
        the timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait in seconds for the message before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :raise octue.exceptions.QuestionNotDelivered: if a delivery acknowledgement is not received in time
        :return None:
        """
        start_time = time.perf_counter()
        attempt = 1

        while True:
            logger.debug("Pulling messages from Google Pub/Sub: attempt %d.", attempt)

            pull_response = self._subscriber.pull(
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

        self._subscriber.acknowledge(request={"subscription": self.subscription.path, "ack_ids": [answer.ack_id]})

        logger.debug(
            "%r received a message related to question %r.",
            self.receiving_service,
            self.subscription.topic.path.split(".")[-1],
        )

        # Get the child's Octue SDK version from the first message.
        if not self._child_sdk_version:
            self._child_sdk_version = answer.message.attributes.get("octue_sdk_version")

            # If the child hasn't provided its Octue SDK version, it's too old to send heartbeats - so, cancel the
            # heartbeat checker to maintain compatibility.
            if not self._child_sdk_version:
                self._heartbeat_checker.cancel()

        message = json.loads(answer.message.data.decode())
        self._waiting_messages[int(message["message_number"])] = message

    def _attempt_to_handle_queued_messages(self, recorded_messages):
        """Attempt to handle messages in the pulled message queue. If these messages aren't consecutive with the last
        handled message (i.e. if messages have been received out of order and the next in-order message hasn't been
        received yet), just return.

        :param list recorded_messages: if recording messages, store them in this
        :return any|None: either a non-`None` result from a message handler or `None` if nothing was returned by the message handlers or if the next in-order message hasn't been received yet
        """
        try:
            while self._waiting_messages:
                message = self._waiting_messages.pop(self._previous_message_number + 1)

                if self.record_messages_to:
                    recorded_messages.append(message)

                result = self._handle_message(message)

                if result is not None:
                    return result

        except KeyError:
            return

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self._previous_message_number += 1

        try:
            return self._message_handlers[message["type"]](message)

        except Exception as error:
            warn_if_incompatible(
                parent_sdk_version=pkg_resources.get_distribution("octue").version,
                child_sdk_version=self._child_sdk_version,
            )

            # Just log a warning if an unknown message type has been received - it's likely not to be a big problem.
            if isinstance(error, KeyError):
                logger.warning(
                    "%r received a message of unknown type %r.",
                    self.receiving_service,
                    message.get("type", "unknown"),
                )
                return

            # Raise all other errors.
            raise error

    def _save_messages(self, recorded_messages):
        """Save the given messages to the JSON file given in `self._record_messages_to`.

        :param list recorded_messages:
        :return None:
        """
        directory_name = os.path.dirname(self.record_messages_to)

        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        with open(self.record_messages_to, "w") as f:
            json.dump(recorded_messages, f)

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        self.received_delivery_acknowledgement = True
        logger.info("%r's question was delivered at %s.", self.receiving_service, message["delivery_time"])

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
        logger.debug("%r received a monitor message.", self.receiving_service)

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
            self.receiving_service,
            self.subscription.topic.path.split(".")[-1],
        )

        if message["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(message["output_manifest"], from_string=True)

        return {"output_values": message["output_values"], "output_manifest": output_manifest}
