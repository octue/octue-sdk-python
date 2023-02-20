import json
import logging
import math
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
    """A handler for Google Pub/Sub messages received via a pull subscription that ensures messages are handled in the
    order they were sent.

    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param octue.cloud.pub_sub.service.Service receiving_service: the service that's receiving the messages
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_messages: if `True`, record received messages in the `received_messages` attribute
    :param str service_name: an arbitrary name to refer to the service subscribed to by (used for labelling its remote log messages)
    :param dict|None message_handlers: a mapping of message type names to callables that handle each type of message. The handlers should not mutate the messages.
    :return None:
    """

    def __init__(
        self,
        subscription,
        receiving_service,
        handle_monitor_message=None,
        record_messages=True,
        service_name="REMOTE",
        message_handlers=None,
    ):
        self.subscription = subscription
        self.receiving_service = receiving_service
        self.handle_monitor_message = handle_monitor_message
        self.record_messages = record_messages
        self.service_name = service_name

        self.question_uuid = self.subscription.topic.path.split(".")[-1]
        self.handled_messages = []
        self.received_response_from_child = None
        self._subscriber = SubscriberClient()
        self._child_sdk_version = None
        self._heartbeat_checker = None
        self._last_heartbeat = None
        self._alive = True
        self._start_time = None
        self._waiting_messages = None
        self._previous_message_number = -1
        self._earliest_message_number_received = math.inf

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
    def total_run_time(self):
        """Get the amount of time elapsed since `self.handle_messages` was called. If it hasn't been called yet, it will
        be `None`.

        :return float|None: the amount of time since `self.handle_messages` was called (in seconds)
        """
        if self._start_time is None:
            return

        return time.perf_counter() - self._start_time

    @property
    def _time_since_last_heartbeat(self):
        """Get the time period since the last heartbeat was received.

        :return datetime.timedelta|None:
        """
        if not self._last_heartbeat:
            return None

        return datetime.now() - self._last_heartbeat

    def handle_messages(
        self,
        timeout=60,
        delivery_acknowledgement_timeout=120,
        maximum_heartbeat_interval=300,
        skip_first_messages_after=60,
    ):
        """Pull messages and handle them in the order they were sent until a result is returned by a message handler,
        then return that result.

        :param float|None timeout: how long to wait for an answer before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :param int|float maximum_heartbeat_interval: the maximum amount of time (in seconds) allowed between child heartbeats before an error is raised
        :param int|float skip_first_messages_after: the number of seconds after which to skip the first n messages if they haven't arrived but subsequent messages have
        :raise TimeoutError: if the timeout is exceeded before receiving the final message
        :return dict: the first result returned by a message handler
        """
        self._start_time = time.perf_counter()
        self.received_response_from_child = False
        self._waiting_messages = {}
        self._previous_message_number = -1

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

                result = self._attempt_to_handle_queued_messages(skip_first_messages_after)

                if result is not None:
                    return result

        finally:
            self._heartbeat_checker.cancel()
            self._subscriber.close()

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

        # Get the total run time once in case it's very close to the timeout - this rules out a negative pull timeout
        # being returned below.
        total_run_time = self.total_run_time

        if total_run_time > timeout:
            raise TimeoutError(
                f"No final answer received from topic {self.subscription.topic.path!r} after {timeout} seconds."
            )

        return timeout - total_run_time

    def _pull_and_enqueue_message(self, timeout, delivery_acknowledgement_timeout):
        """Pull a message from the subscription and enqueue it in `self._waiting_messages`, raising a `TimeoutError` if
        the timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait in seconds for the message before raising a `TimeoutError`
        :param float delivery_acknowledgement_timeout: how long to wait for a delivery acknowledgement before raising `QuestionNotDelivered`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :raise octue.exceptions.QuestionNotDelivered: if a delivery acknowledgement is not received in time
        :return None:
        """
        pull_start_time = time.perf_counter()
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

                pull_run_time = time.perf_counter() - pull_start_time

                if timeout is not None and pull_run_time > timeout:
                    raise TimeoutError(
                        f"No message received from topic {self.subscription.topic.path!r} after {timeout} seconds.",
                    )

                if not self.received_response_from_child and self.total_run_time > delivery_acknowledgement_timeout:
                    raise QuestionNotDelivered(
                        f"No delivery acknowledgement received for topic {self.subscription.topic.path!r} after "
                        f"{delivery_acknowledgement_timeout} seconds."
                    )

        self._subscriber.acknowledge(request={"subscription": self.subscription.path, "ack_ids": [answer.ack_id]})
        logger.debug("%r received a message related to question %r.", self.receiving_service, self.question_uuid)

        # Get the child's Octue SDK version from the first message.
        if not self._child_sdk_version:
            self._child_sdk_version = answer.message.attributes.get("octue_sdk_version")

            # If the child hasn't provided its Octue SDK version, it's too old to send heartbeats - so, cancel the
            # heartbeat checker to maintain compatibility.
            if not self._child_sdk_version:
                self._heartbeat_checker.cancel()

        message = json.loads(answer.message.data.decode())

        message_number = int(message["message_number"])
        self._waiting_messages[message_number] = message
        self._earliest_message_number_received = min(self._earliest_message_number_received, message_number)

    def _attempt_to_handle_queued_messages(self, skip_first_messages_after=60):
        """Attempt to handle messages in the pulled message queue. If these messages aren't consecutive with the last
        handled message (i.e. if messages have been received out of order and the next in-order message hasn't been
        received yet), just return. After the given amount of time, if the first n messages haven't arrived but
        subsequent ones have, skip to the earliest received message and continue from there.

        :param int|float skip_first_messages_after: the number of seconds after which to skip the first n messages if they haven't arrived but subsequent messages have
        :return any|None: either a non-`None` result from a message handler or `None` if nothing was returned by the message handlers or if the next in-order message hasn't been received yet
        """
        while self._waiting_messages:
            try:
                message = self._waiting_messages.pop(self._previous_message_number + 1)

            except KeyError:

                if self.total_run_time > skip_first_messages_after and self._previous_message_number == -1:
                    message = self._get_and_start_from_earliest_received_message(skip_first_messages_after)

                    if not message:
                        return

                else:
                    return

            result = self._handle_message(message)

            if result is not None:
                return result

    def _get_and_start_from_earliest_received_message(self, skip_first_messages_after):
        """Get the earliest received message from the waiting message queue and set the message handler up to start from
        it instead of the first message sent by the child.

        :param int|float skip_first_messages_after: the number of seconds after which to skip the first n messages if they haven't arrived but subsequent messages have
        :return dict|None:
        """
        try:
            message = self._waiting_messages.pop(self._earliest_message_number_received)
        except KeyError:
            return

        self._previous_message_number = self._earliest_message_number_received - 1

        logger.warning(
            "%r: The first %d messages for question %r weren't received after %ds - skipping to the "
            "earliest received message (message number %d).",
            self.receiving_service,
            self._earliest_message_number_received,
            self.question_uuid,
            skip_first_messages_after,
            self._earliest_message_number_received,
        )

        return message

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self.received_response_from_child = True
        self._previous_message_number += 1

        if self.record_messages:
            self.handled_messages.append(message)

        try:
            return self._message_handlers[message["type"]](message)
        except Exception as error:
            self._warn_of_or_raise_invalid_message_error(message, error)

    def _warn_of_or_raise_invalid_message_error(self, message, error):
        """Issue a warning if the error is due to a message of an unknown type or raise the error if it's due to
        anything else. Issue an additional warning if the parent and child SDK versions are incompatible.

        :param dict message: the message whose handling has caused an error
        :param Exception error: the error caused by handling the message
        :return None:
        """
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

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        logger.info("%r's question was delivered at %s.", self.receiving_service, message["delivery_time"])

    def _handle_heartbeat(self, message):
        """Record the time the heartbeat was received.

        :param dict message:
        :return None:
        """
        self._last_heartbeat = datetime.now()
        logger.info("Heartbeat received from service %r for question %r.", self.service_name, self.question_uuid)

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
        logger.info("%r received an answer to question %r.", self.receiving_service, self.question_uuid)

        if message["output_manifest"] is None:
            output_manifest = None
        else:
            output_manifest = Manifest.deserialise(message["output_manifest"], from_string=True)

        return {"output_values": message["output_values"], "output_manifest": output_manifest}
