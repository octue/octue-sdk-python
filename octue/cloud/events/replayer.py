import logging

from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import ServiceBackend


logger = logging.getLogger(__name__)


class EventReplayer(AbstractEventHandler):
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
        self.waiting_events = {}
        self._previous_event_number = -1

        for event in events:
            self._extract_and_enqueue_event(event)

        self._earliest_waiting_event_number = min(self.waiting_events.keys())
        return self._attempt_to_handle_waiting_events()

    def _extract_event_and_attributes(self, event):
        event["attributes"]["message_number"] = int(event["attributes"]["message_number"])
        return event["event"], event["attributes"]
