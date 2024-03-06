import abc
import importlib.metadata
import logging
import math
import os
import re
import time
from datetime import datetime

from octue.cloud import EXCEPTIONS_MAPPING
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA, is_event_valid
from octue.definitions import GOOGLE_COMPUTE_PROVIDERS
from octue.log_handlers import COLOUR_PALETTE
from octue.resources.manifest import Manifest


logger = logging.getLogger(__name__)


if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") in GOOGLE_COMPUTE_PROVIDERS:
    # Google Cloud logs don't support colour currently - provide a no-operation function.
    colourise = lambda string, text_colour=None, background_colour=None: string
else:
    from octue.utils.colour import colourise


PARENT_SDK_VERSION = importlib.metadata.version("octue")


class EventHandler:
    question_uuid: str

    def __init__(
        self,
        receiving_service,
        handle_monitor_message=None,
        record_messages=True,
        service_name="REMOTE",
        message_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        skip_missing_messages_after=10,
    ):
        self.receiving_service = receiving_service
        self.handle_monitor_message = handle_monitor_message
        self.record_messages = record_messages
        self.service_name = service_name
        self.schema = schema

        self.waiting_messages = None
        self.handled_messages = []
        self._previous_message_number = -1
        self._child_sdk_version = None

        self.skip_missing_messages_after = skip_missing_messages_after
        self._missing_message_detection_time = None
        self._earliest_waiting_message_number = math.inf

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
    def time_since_missing_message(self):
        """Get the amount of time elapsed since the last missing message was detected. If no missing messages have been
        detected or they've already been skipped past, `None` is returned.

        :return float|None:
        """
        if self._missing_message_detection_time is None:
            return None

        return time.perf_counter() - self._missing_message_detection_time

    @abc.abstractmethod
    def _extract_event_and_attributes(self, event):
        pass

    def _extract_and_enqueue_event(self, event):
        """Extract an event from the Pub/Sub message and add it to `self.waiting_messages`.

        :param dict event:
        :return None:
        """
        logger.debug("%r received a message related to question %r.", self.receiving_service, self.question_uuid)
        event, attributes = self._extract_event_and_attributes(event)

        if not is_event_valid(
            event=event,
            attributes=attributes,
            receiving_service=self.receiving_service,
            parent_sdk_version=PARENT_SDK_VERSION,
            child_sdk_version=attributes.get("version"),
            schema=self.schema,
        ):
            return

        # Get the child's Octue SDK version from the first message.
        if not self._child_sdk_version:
            self._child_sdk_version = attributes["version"]

        message_number = attributes["message_number"]

        if message_number in self.waiting_messages:
            logger.warning(
                "%r: Message with duplicate message number %d received for question %s - overwriting original message.",
                self.receiving_service,
                message_number,
                self.question_uuid,
            )

        self.waiting_messages[message_number] = event

    def _attempt_to_handle_waiting_messages(self):
        """Attempt to handle messages waiting in `self.waiting_messages`. If these messages aren't consecutive to the
        last handled message (i.e. if messages have been received out of order and the next in-order message hasn't been
        received yet), just return. After the missing message wait time has passed, if this set of missing messages
        haven't arrived but subsequent ones have, skip to the earliest waiting message and continue from there.

        :return any|None: either a non-`None` result from a message handler or `None` if nothing was returned by the message handlers or if the next in-order message hasn't been received yet
        """
        while self.waiting_messages:
            try:
                # If the next consecutive message has been received:
                message = self.waiting_messages.pop(self._previous_message_number + 1)

            # If the next consecutive message hasn't been received:
            except KeyError:
                # Start the missing message timer if it isn't already running.
                if self._missing_message_detection_time is None:
                    self._missing_message_detection_time = time.perf_counter()

                if self.time_since_missing_message > self.skip_missing_messages_after:
                    message = self._skip_to_earliest_waiting_message()

                    # Declare there are no more missing messages.
                    self._missing_message_detection_time = None

                    if not message:
                        return

                else:
                    return

            result = self._handle_message(message)

            if result is not None:
                return result

    def _skip_to_earliest_waiting_message(self):
        """Get the earliest waiting message and set the message handler up to continue from it.

        :return dict|None:
        """
        try:
            message = self.waiting_messages.pop(self._earliest_waiting_message_number)
        except KeyError:
            return

        number_of_missing_messages = self._earliest_waiting_message_number - self._previous_message_number - 1

        # Let the message handler know it can handle the next earliest message.
        self._previous_message_number = self._earliest_waiting_message_number - 1

        logger.warning(
            "%r: %d consecutive messages missing for question %r after %ds - skipping to next earliest waiting message "
            "(message %d).",
            self.receiving_service,
            number_of_missing_messages,
            self.question_uuid,
            self.skip_missing_messages_after,
            self._earliest_waiting_message_number,
        )

        return message

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self._previous_message_number += 1

        if self.record_messages:
            self.handled_messages.append(message)

        handler = self._message_handlers[message["kind"]]
        return handler(message)

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        logger.info("%r's question was delivered at %s.", self.receiving_service, message["datetime"])

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
            self.handle_monitor_message(message["data"])

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
            f"[{self.service_name} | analysis-{self.question_uuid}]",
            text_colour=self._log_message_colours[0],
        )

        # Colour any analysis sections from children of the immediate child with the rest of the colour palette.
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
                "".join(message["exception_traceback"]),
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

        if message.get("output_manifest"):
            output_manifest = Manifest.deserialise(message["output_manifest"])
        else:
            output_manifest = None

        return {"output_values": message.get("output_values"), "output_manifest": output_manifest}
