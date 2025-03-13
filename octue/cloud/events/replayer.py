import logging

from octue.cloud.events.attributes import ResponseAttributes
from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA

logger = logging.getLogger(__name__)


class EventReplayer(AbstractEventHandler):
    """A replayer for events retrieved asynchronously from storage. Note that events aren't validated by default as the
    main use case for this class is to replay already-validated events from the event store.

    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict|str schema: the JSON schema to validate events against
    :param bool include_service_metadata_in_logs: if `True`, include the SRUIDs and question UUIDs of the service revisions involved in the question to the start of the log message
    :param str|None exclude_logs_containing: if provided, skip handling log messages containing this string
    :param bool only_handle_result: if `True`, skip non-result events and only handle the "result" event if present (turning this on speeds up event handling)
    :param bool validate_events: if `True`, validate events before attempting to handle them (this is off by default to speed up event handling)
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
        validate_events=False,
    ):
        event_handlers = event_handlers or {
            "question": self._handle_question,
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "heartbeat": self._handle_heartbeat,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

        super().__init__(
            handle_monitor_message=handle_monitor_message,
            record_events=record_events,
            event_handlers=event_handlers,
            schema=schema,
            include_service_metadata_in_logs=include_service_metadata_in_logs,
            exclude_logs_containing=exclude_logs_containing,
            only_handle_result=only_handle_result,
            validate_events=validate_events,
        )

    def handle_events(self, events):
        """Handle the given events and return a handled "result" event if one is present.

        :param iter(dict) events: the events to handle
        :return dict|None: the handled "result" event if present
        """
        super().handle_events()

        for event in events:
            # Skip validation and handling of other event kinds if only the result event is wanted.
            if self.only_handle_result and event.get("event", {}).get("kind") != "result":
                continue

            event, attributes = self._extract_and_validate_event(event)

            # Skip the event if it fails validation.
            if not event:
                continue

            result = self._handle_event(event, attributes)

            if result:
                return result

        logger.warning("No result was found for this question.")

    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from the event container.

        :param dict container: the container of the event
        :return (any, dict): the event and its attributes
        """
        return container.get("event", {}), ResponseAttributes(**container["attributes"])

    def _handle_question(self, event, attributes):
        """Log that the question was sent.

        :param dict event:
        :param dict attributes: the event's attributes
        :return None:
        """
        logger.info(
            "%r asked a question %r to service %r.",
            attributes["sender"],
            attributes["question_uuid"],
            attributes["recipient"],
        )
