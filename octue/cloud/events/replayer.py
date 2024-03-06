import logging

from octue.cloud.events.event_handler import EventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.cloud.pub_sub.service import Service
from octue.resources.service_backends import ServiceBackend


logger = logging.getLogger(__name__)


class EventReplayer(EventHandler):
    def __init__(
        self,
        receiving_service=None,
        handle_monitor_message=None,
        record_messages=True,
        service_name="REMOTE",
        message_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
    ):
        super().__init__(
            receiving_service or Service(backend=ServiceBackend(), service_id="local/local:local"),
            handle_monitor_message=handle_monitor_message,
            record_messages=record_messages,
            service_name=service_name,
            message_handlers=message_handlers,
            schema=schema,
            skip_missing_messages_after=0,
        )

    def handle_events(self, events):
        self.question_uuid = events[0]["attributes"]["question_uuid"]
        self.waiting_messages = {}
        self._previous_message_number = -1

        for event in events:
            self._extract_and_enqueue_event(event)

        self._earliest_waiting_message_number = min(self.waiting_messages.keys())
        return self._attempt_to_handle_waiting_messages()

    def _extract_event_and_attributes(self, event):
        event["attributes"]["message_number"] = int(event["attributes"]["message_number"])
        return event["event"], event["attributes"]
