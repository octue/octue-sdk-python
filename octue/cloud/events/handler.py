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


class AbstractEventHandler:
    """An abstract event handler for Octue service events that:
    - Provide handlers for the Octue service event kinds (see https://strands.octue.com/octue/service-communication)
    - Handles received events in the order specified by the `order` attribute
    - Skips missing events after a set time and carries on handling from the next available event

    To create a concrete handler for a specific service/communication backend synchronously or asynchronously, inherit
    from this class and add the `handle_events` and `_extract_event_and_attributes` methods.

    :param octue.cloud.pub_sub.service.Service recipient: the `Service` instance that's receiving the events
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict schema: the JSON schema to validate events against
    :param int|float skip_missing_events_after: the number of seconds after which to skip any events if they haven't arrived but subsequent events have
    :param bool only_handle_result: if `True`, skip non-result events and only handle the "result" event when received
    :return None:
    """

    def __init__(
        self,
        recipient,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        skip_missing_events_after=10,
        only_handle_result=False,
    ):
        self.recipient = recipient
        self.handle_monitor_message = handle_monitor_message
        self.record_events = record_events
        self.schema = schema
        self.only_handle_result = only_handle_result

        # These are set when the first event is received.
        self.question_uuid = None
        self.child_sruid = None
        self.child_sdk_version = None

        self.waiting_events = None
        self.handled_events = []
        self._previous_event_number = -1
        self._start_time = None

        self.skip_missing_events_after = skip_missing_events_after
        self._missing_event_detection_time = None
        self._earliest_waiting_event_number = math.inf

        self._event_handlers = event_handlers or {
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "heartbeat": self._handle_heartbeat,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

        self._log_message_colours = [COLOUR_PALETTE[1], *COLOUR_PALETTE[3:]]

    @property
    def awaiting_missing_event(self):
        """Check if the event handler is currently waiting for a missing event.

        :return bool: `True` if the event handler is currently waiting for a missing event
        """
        return self._missing_event_detection_time is not None

    @property
    def time_since_missing_event(self):
        """Get the amount of time elapsed since the last missing event was detected. If no missing events have been
        detected or they've already been skipped, `None` is returned.

        :return float|None:
        """
        if not self.awaiting_missing_event:
            return None

        return time.perf_counter() - self._missing_event_detection_time

    @abc.abstractmethod
    def handle_events(self, *args, **kwargs):
        """Handle events and return a handled "result" event once one is received. This method must be overridden but
        can have any arguments. The first thing it should do is call `super().handle_events()`.

        :return dict: the handled final result
        """
        self.reset()

    def reset(self):
        """Reset the handler to be ready to handle a new stream of events.

        :return None:
        """
        self._start_time = time.perf_counter()
        self.waiting_events = {}
        self._previous_event_number = -1

    @abc.abstractmethod
    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from the event container. This method must be overridden.

        :param any container: the container of the event (e.g. a Pub/Sub message)
        :return (any, dict): the event and its attributes (both must conform to the service communications event schema)
        """
        pass

    def _extract_and_enqueue_event(self, container):
        """Extract an event from its container, validate it, and add it to `self.waiting_events` if it's valid.

        :param any container: the container of the event (e.g. a Pub/Sub message)
        :return None:
        """
        try:
            event, attributes = self._extract_event_and_attributes(container)
        except Exception:
            event = None
            attributes = {}

        # Don't assume the presence of specific attributes before validation.
        child_sdk_version = attributes.get("sender_sdk_version")

        if not is_event_valid(
            event=event,
            attributes=attributes,
            recipient=self.recipient,
            parent_sdk_version=PARENT_SDK_VERSION,
            child_sdk_version=child_sdk_version,
            schema=self.schema,
        ):
            return

        # Get the child's SRUID and Octue SDK version from the first event.
        if not self.child_sdk_version:
            self.question_uuid = attributes["question_uuid"]
            self.child_sruid = attributes["sender"]
            self.child_sdk_version = attributes["sender_sdk_version"]

        logger.debug("%r: Received an event related to question %r.", self.recipient, self.question_uuid)
        order = attributes["order"]

        if order in self.waiting_events:
            logger.warning(
                "%r: Event with duplicate order %d received for question %r - overwriting original event.",
                self.recipient,
                order,
                self.question_uuid,
            )

        self.waiting_events[order] = event

    def _attempt_to_handle_waiting_events(self):
        """Attempt to handle any events waiting in `self.waiting_events`. If these events aren't consecutive to the
        last handled event (i.e. if events have been received out of order and the next in-order event hasn't been
        received yet), just return. After the missing event wait time has passed, if this set of missing events
        haven't arrived but subsequent ones have, skip to the earliest waiting event and continue from there.

        :return any|None: either a handled non-`None` "result" event, or `None` if nothing was returned by the event handlers or if the next in-order event hasn't been received yet
        """
        # Handle the case where no events (or no valid events) have been received.
        if not self.waiting_events:
            logger.debug("No events (or no valid events) were received.")
            return

        self._earliest_waiting_event_number = min(self.waiting_events.keys())

        while self.waiting_events:
            try:
                # If the next consecutive event has been received:
                event = self.waiting_events.pop(self._previous_event_number + 1)

            # If the next consecutive event hasn't been received:
            except KeyError:
                # Start the missing event timer if it isn't already running.
                if not self.awaiting_missing_event:
                    self._missing_event_detection_time = time.perf_counter()

                if self.time_since_missing_event > self.skip_missing_events_after:
                    event = self._skip_to_earliest_waiting_event()

                    # Declare there are no more missing events.
                    self._missing_event_detection_time = None

                    if not event:
                        return

                else:
                    return

            result = self._handle_event(event)

            if result is not None:
                return result

    def _skip_to_earliest_waiting_event(self):
        """Get the earliest waiting event and set the event handler up to continue from it.

        :return dict|None: the earliest waiting event if there is one
        """
        try:
            event = self.waiting_events.pop(self._earliest_waiting_event_number)
        except KeyError:
            return

        number_of_missing_events = self._earliest_waiting_event_number - self._previous_event_number - 1

        # Let the event handler know it can handle the next earliest event.
        self._previous_event_number = self._earliest_waiting_event_number - 1

        logger.warning(
            "%r: %d consecutive events missing for question %r after %ds - skipping to next earliest waiting event "
            "(event %d).",
            self.recipient,
            number_of_missing_events,
            self.question_uuid,
            self.skip_missing_events_after,
            self._earliest_waiting_event_number,
        )

        return event

    def _handle_event(self, event):
        """Pass an event to its handler and update the previous event number.

        :param dict event: the event to handle
        :return dict|None: the output of the event (this should be `None` unless the event is a "result" event)
        """
        self._previous_event_number += 1

        if self.record_events:
            self.handled_events.append(event)

        if self.only_handle_result and event["kind"] != "result":
            return

        handler = self._event_handlers[event["kind"]]
        return handler(event)

    def _handle_delivery_acknowledgement(self, event):
        """Log that the question was delivered.

        :param dict event:
        :return None:
        """
        logger.info("%r's question was delivered at %s.", self.recipient, event["datetime"])

    def _handle_heartbeat(self, event):
        """Record the time the heartbeat was received.

        :param dict event:
        :return None:
        """
        self._last_heartbeat = datetime.now()
        logger.info(
            "%r: Received a heartbeat from service %r for question %r.",
            self.recipient,
            self.child_sruid,
            self.question_uuid,
        )

    def _handle_monitor_message(self, event):
        """Send the monitor message to the handler if one has been provided.

        :param dict event:
        :return None:
        """
        logger.debug(
            "%r: Received a monitor message from service %r for question %r.",
            self.recipient,
            self.child_sruid,
            self.question_uuid,
        )

        if self.handle_monitor_message is not None:
            self.handle_monitor_message(event["data"])

    def _handle_log_message(self, event):
        """Deserialise the event into a log record and pass it to the local log handlers. The child's SRUID and the
        question UUID are added to the start of the log message, and the SRUIDs of any subchildren called by the child
        are each coloured differently.

        :param dict event:
        :return None:
        """
        record = logging.makeLogRecord(event["log_record"])

        # Add information about the immediate child sending the event and colour it with the first colour in the
        # colour palette.
        immediate_child_analysis_section = colourise(
            f"[{self.child_sruid} | analysis-{self.question_uuid}]",
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

    def _handle_exception(self, event):
        """Raise the exception from the child.

        :param dict event:
        :raise Exception:
        :return None:
        """
        exception_message = "\n\n".join(
            (
                event["exception_message"],
                f"The following traceback was captured from the remote service {self.child_sruid!r}:",
                "".join(event["exception_traceback"]),
            )
        )

        try:
            exception_type = EXCEPTIONS_MAPPING[event["exception_type"]]

        # Allow unknown exception types to still be raised.
        except KeyError:
            exception_type = type(event["exception_type"], (Exception,), {})

        raise exception_type(exception_message)

    def _handle_result(self, event):
        """Extract any output values and output manifest from the result, deserialising the manifest if present.

        :param dict event:
        :return dict:
        """
        logger.info("%r: Received an answer to question %r.", self.recipient, self.question_uuid)

        if event.get("output_manifest"):
            output_manifest = Manifest.deserialise(event["output_manifest"])
        else:
            output_manifest = None

        return {"output_values": event.get("output_values"), "output_manifest": output_manifest}
