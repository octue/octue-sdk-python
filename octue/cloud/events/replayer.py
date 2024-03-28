import logging

from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import ServiceBackend


logger = logging.getLogger(__name__)


class EventReplayer(AbstractEventHandler):
    """A replayer for events retrieved asynchronously from some kind of storage.

    :param octue.cloud.pub_sub.service.Service receiving_service: the service that's receiving the events
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers should not mutate the events.
    :param dict|str schema: the JSON schema (or URI of one) to validate events against
    :param bool only_handle_result: if `True`, skip non-result events and only handle the result event
    :return None:
    """

    def __init__(
        self,
        receiving_service=None,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        only_handle_result=False,
    ):
        super().__init__(
            receiving_service or Service(backend=ServiceBackend(), service_id="local/local:local"),
            handle_monitor_message=handle_monitor_message,
            record_events=record_events,
            event_handlers=event_handlers,
            schema=schema,
            skip_missing_events_after=0,
            only_handle_result=only_handle_result,
        )

    def handle_events(self, events):
        """Handle the given events and return a handled "result" event if one is reached.

        :return dict: the handled final result
        """
        self.waiting_events = {}
        self._previous_event_number = -1

        for event in events:
            self._extract_and_enqueue_event(event)

        # Handle the case where no events (or no valid events) have been received.
        if self.waiting_events:
            self._earliest_waiting_event_number = min(self.waiting_events.keys())
            return self._attempt_to_handle_waiting_events()

    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from the event container.

        :param dict container: the container of the event
        :return (any, dict): the event and its attributes
        """
        container["attributes"]["order"] = int(container["attributes"]["order"])
        return container["event"], container["attributes"]
