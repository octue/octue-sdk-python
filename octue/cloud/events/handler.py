import abc
from datetime import datetime
import logging
import os
import re
import time

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


class AbstractEventHandler:
    """An abstract event handler for Octue service events that:
    - Provide handlers for the Octue service event kinds (see https://strands.octue.com/octue/service-communication)
    - Handles received events in the order received

    To create a concrete handler for a specific service/communication backend synchronously or asynchronously, inherit
    from this class and add the `handle_events` and `_extract_event_and_attributes` methods.

    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict schema: the JSON schema to validate events against
    :param bool include_service_metadata_in_logs: if `True`, include the SRUIDs and question UUIDs of the service revisions involved in the question to the start of the log message
    :param str|None exclude_logs_containing: if provided, skip handling log messages containing this string
    :param bool only_handle_result: if `True`, skip handling non-result events and only handle the "result" event when received (turning this on speeds up event handling)
    :param bool validate_events: if `True`, validate events before attempting to handle them (turning this off speeds up event handling)
    :return None:
    """

    def __init__(
        self,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        include_service_metadata_in_logs=True,
        exclude_logs_containing=None,
        only_handle_result=False,
        validate_events=True,
    ):
        self.handle_monitor_message = handle_monitor_message
        self.record_events = record_events
        self.schema = schema
        self.include_service_metadata_in_logs = include_service_metadata_in_logs
        self.exclude_logs_containing = exclude_logs_containing
        self.only_handle_result = only_handle_result
        self.validate_events = validate_events

        self.handled_events = []
        self._start_time = None

        self._event_handlers = event_handlers or {
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "heartbeat": self._handle_heartbeat,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

        self._log_message_colours = [COLOUR_PALETTE[1], *COLOUR_PALETTE[3:]]

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

    @abc.abstractmethod
    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from the event container. This method must be overridden.

        :param any container: the container of the event (e.g. a Pub/Sub message)
        :return (any, dict): the event and its attributes (both must conform to the service communications event schema)
        """
        pass

    def _extract_and_validate_event(self, container):
        """Extract an event from its container, validate it, and return it.

        :param any container: the container of the event (e.g. a Pub/Sub message)
        :return (dict, dict)|(None, None): the event and its attributes if they're valid, or `(None, None)` if they're invalid
        """
        try:
            event, attributes = self._extract_event_and_attributes(container)
        except Exception as e:
            logger.exception(e)
            event = None
            attributes = {}

        if self.validate_events and not is_event_valid(
            event=event,
            attributes=attributes,
            recipient=attributes.recipient,
            schema=self.schema,
        ):
            return (None, None)

        logger.debug("%r: Received an event related to question %r.", attributes.recipient, attributes.question_uuid)
        return (event, attributes)

    def _handle_event(self, event, attributes):
        """Pass an event to its handler and record it if appropriate.

        :param dict event: the event to handle
        :param dict attributes: the event's attributes
        :return dict|None: the output of the event (this should be `None` unless the event is a "result" event)
        """
        if self.record_events:
            self.handled_events.append({"event": event, "attributes": attributes.to_minimal_dict()})

        if self.only_handle_result and event["kind"] != "result":
            return

        handler = self._event_handlers[event["kind"]]
        return handler(event, attributes)

    def _handle_delivery_acknowledgement(self, event, attributes):
        """Log that the question was delivered.

        :param dict event:
        :param dict attributes: the event's attributes
        :return None:
        """
        logger.info("%rs question was delivered at %s.", attributes.recipient, attributes.datetime)

    def _handle_heartbeat(self, event, attributes):
        """Record the time the heartbeat was received.

        :param dict event:
        :param dict attributes: the event's attributes
        :return None:
        """
        self._last_heartbeat = datetime.now()

        logger.info(
            "%r: Received a heartbeat from service %r for question %r.",
            attributes.recipient,
            attributes.sender,
            attributes.question_uuid,
        )

    def _handle_monitor_message(self, event, attributes):
        """Send the monitor message to the handler if one has been provided.

        :param dict event:
        :param dict attributes: the event's attributes
        :return None:
        """
        logger.debug(
            "%r: Received a monitor message from service %r for question %r.",
            attributes.recipient,
            attributes.sender,
            attributes.question_uuid,
        )

        if self.handle_monitor_message is not None:
            self.handle_monitor_message(event["data"])

    def _handle_log_message(self, event, attributes):
        """Deserialise the event into a log record and pass it to the local log handlers. The child's SRUID and the
        question UUID are added to the start of the log message, and the SRUIDs of any subchildren called by the child
        are each coloured differently.

        :param dict event:
        :param dict attributes: the event's attributes
        :return None:
        """
        if self.exclude_logs_containing and self.exclude_logs_containing in event["log_record"]["msg"]:
            return

        record = logging.makeLogRecord(event["log_record"])

        # Split the log message into its parts.
        subchild_analysis_sections = [section.strip("[") for section in re.split("] ", record.msg)]
        final_message = subchild_analysis_sections.pop(-1)

        if self.include_service_metadata_in_logs:
            # Get information about the immediate child sending the event and colour it with the first colour in the
            # colour palette.
            immediate_child_analysis_section = colourise(
                f"[{attributes.sender} | {attributes.question_uuid}]",
                text_colour=self._log_message_colours[0],
            )

            # Colour any analysis sections from children of the immediate child with the rest of the colour palette.
            for i in range(len(subchild_analysis_sections)):
                subchild_analysis_sections[i] = colourise(
                    "[" + subchild_analysis_sections[i] + "]",
                    text_colour=self._log_message_colours[1:][i % len(self._log_message_colours[1:])],
                )

            record.msg = " ".join([immediate_child_analysis_section, *subchild_analysis_sections, final_message])

        else:
            record.msg = final_message

        logger.handle(record)

    def _handle_exception(self, event, attributes):
        """Raise the exception from the child.

        :param dict event:
        :param dict attributes: the event's attributes
        :raise Exception:
        :return None:
        """
        exception_message = "\n\n".join(
            (
                event["exception_message"],
                f"The following traceback was captured from the remote service {attributes.sender!r}:",
                "".join(event["exception_traceback"]),
            )
        )

        try:
            exception_type = EXCEPTIONS_MAPPING[event["exception_type"]]

        # Allow unknown exception types to still be raised.
        except KeyError:
            exception_type = type(event["exception_type"], (Exception,), {})

        raise exception_type(exception_message)

    def _handle_result(self, event, attributes):
        """Extract any output values and output manifest from the result, deserialising the manifest if present.

        :param dict event:
        :param dict attributes: the event's attributes
        :return dict:
        """
        logger.info("%r: Received an answer to question %r.", attributes.recipient, attributes.question_uuid)

        if event.get("output_manifest"):
            output_manifest = Manifest.deserialise(event["output_manifest"])
        else:
            output_manifest = None

        return {"output_values": event.get("output_values"), "output_manifest": output_manifest}
