import logging

from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA


logger = logging.getLogger(__name__)


class EventReplayer(AbstractEventHandler):
    """A replayer for events retrieved asynchronously from storage. Missing events are immediately skipped.

    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict|str schema: the JSON schema to validate events against
    :param bool only_handle_result: if `True`, skip non-result events and only handle the "result" event if present
    :return None:
    """

    def __init__(
        self,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        only_handle_result=False,
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
            only_handle_result=only_handle_result,
        )

    def handle_events(self, events):
        """Handle the given events in datetime order and return a handled "result" event if one is present.

        :param iter(dict) events: the events to handle
        :return dict|None: the handled "result" event if present
        """
        super().handle_events()

        # Order by datetime but don't assume the presence of specific attributes before validation.
        for event in sorted(events, key=lambda event: event.get("attributes", {}).get("datetime")):
            event, attributes = self._extract_and_validate_event(event)

            # Skip the event if it fails validation.
            if not event:
                continue

            result = self._handle_event(event, attributes)

            if result:
                return result

        logger.debug("No events (or no valid events) were received.")

    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from the event container.

        :param dict container: the container of the event
        :return (any, dict): the event and its attributes
        """
        container["attributes"]["order"] = int(container["attributes"]["order"])
        return container["event"], container["attributes"]

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
