from datetime import datetime, timedelta
from functools import cached_property
import json
import logging
import time

from google.api_core import retry
from google.cloud.pubsub_v1 import SubscriberClient

from octue.cloud.events.attributes import ResponseAttributes
from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.definitions import DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.objects import get_nested_attribute
from octue.utils.threads import RepeatingTimer

logger = logging.getLogger(__name__)

MAX_SIMULTANEOUS_MESSAGES_PULL = 50


def extract_event(message):
    """Extract a Twined service event from a dictionary or Pub/Sub message.

    :param dict|google.cloud.pubsub_v1.subscriber.message.Message message: the message in dictionary format or direct Google Pub/Sub format
    :return dict: the extracted event
    """
    # Support already-extracted questions (e.g. from the `octue question ask local` CLI command).
    if isinstance(message, dict) and "event" in message:
        return message["event"]

    # Extract event directly from Pub/Sub.
    return json.loads(message.data.decode(), cls=OctueJSONDecoder)


class GoogleCloudPubSubEventHandler(AbstractEventHandler):
    """A synchronous handler for events received as Google Pub/Sub messages from a pull subscription.

    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict|str schema: the JSON schema to validate events against
    :param bool include_service_metadata_in_logs: if `True`, include the SRUIDs and question UUIDs of the service revisions involved in the question to the start of the log message
    :param str|None exclude_logs_containing: if provided, skip handling log messages containing this string
    :param bool only_handle_result: if `True`, skip non-result events and only handle the "result" event if present (turning this on speeds up event handling)
    :param bool validate_events: if `True`, validate events before attempting to handle them (turn this off to speed up event handling at risk of failure if an invalid event is received)
    :return None:
    """

    def __init__(
        self,
        subscription,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        include_service_metadata_in_logs=True,
        exclude_logs_containing=None,
        only_handle_result=False,
        validate_events=True,
    ):
        self.subscription = subscription

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

        self._heartbeat_checker = None
        self._last_heartbeat = None
        self._alive = True

    @cached_property
    def subscriber(self):
        """Get or instantiate the subscriber client. The client isn't instantiated until this property is called for the
        first time. This allows checking for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to be put off
        until it's needed.

        :return google.cloud.pubsub_v1.SubscriberClient:
        """
        return SubscriberClient()

    @property
    def total_run_time(self):
        """The amount of time elapsed since `self.handle_events` was called. If it hasn't been called yet, this is
        `None`.

        :return float|None: the amount of time [s] since `self.handle_events` was called
        """
        if self._start_time is None:
            return None

        return time.perf_counter() - self._start_time

    @property
    def _time_since_last_heartbeat(self):
        """The amount of time since the last heartbeat was received. If no heartbeat has been received, this is `None`.

        :return datetime.timedelta|None:
        """
        if not self._last_heartbeat:
            return None

        return datetime.now() - self._last_heartbeat

    def handle_events(self, timeout=60, maximum_heartbeat_interval=DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL):
        """Pull events from the subscription and handle them in the order they were sent until a "result" event is
        handled, then return the handled result.

        :param float|None timeout: how long to wait for an answer before raising a `TimeoutError`
        :param int|float maximum_heartbeat_interval: the maximum amount of time [s] allowed between child heartbeats before an error is raised
        :raise TimeoutError: if the timeout is exceeded before receiving the final event
        :return dict: the handled "result" event
        """
        super().handle_events()

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
                events = self._pull_available_events(timeout=pull_timeout)

                for event, attributes in events:
                    # Skip the event if it fails validation.
                    if not event:
                        continue

                    result = self._handle_event(event, attributes)

                    if result:
                        return result

        finally:
            self._heartbeat_checker.cancel()
            self.subscriber.close()

        if self.handled_events:
            last_event = self.handled_events[-1]
            sender = last_event["attributes"]["sender"]
            question_uuid = last_event["attributes"]["question_uuid"]
        else:
            sender = "UNKNOWN"
            question_uuid = "UNKNOWN"

        raise TimeoutError(
            f"No heartbeat has been received from {sender!r} for question {question_uuid} within the maximum allowed "
            f"interval of {maximum_heartbeat_interval}s."
        )

    def _monitor_heartbeat(self, maximum_heartbeat_interval):
        """Change the alive status to `False` and cancel the heartbeat checker if a heartbeat hasn't been received
        within the maximum allowed time interval since the last received heartbeat.

        :param float|int maximum_heartbeat_interval: the maximum amount of time [s] allowed between child heartbeats without raising an error
        :return None:
        """
        maximum_heartbeat_interval = timedelta(seconds=maximum_heartbeat_interval)

        if self._last_heartbeat and self._time_since_last_heartbeat <= maximum_heartbeat_interval:
            self._alive = True
            return

        self._alive = False
        self._heartbeat_checker.cancel()

    def _check_timeout_and_get_pull_timeout(self, timeout):
        """Check if the message handling timeout has been exceeded and, if it hasn't, calculate and return the timeout
        for the next message pull. If the timeout has been exceeded, raise an error.

        :param int|float|None timeout: the timeout [s] for handling all messages, or `None` if there's no timeout
        :raise TimeoutError: if the timeout has been exceeded
        :return int|float|None: the timeout for the next message pull [s], or `None` if there's no timeout
        """
        if timeout is None:
            return None

        # Get the total run time once in case it's very close to the timeout - this rules out a negative pull timeout
        # being returned below.
        total_run_time = self.total_run_time

        if total_run_time > timeout:
            raise TimeoutError(f"No final result received from {self.subscription.topic!r} after {timeout} seconds.")

        return timeout - total_run_time

    def _pull_available_events(self, timeout):
        """Pull as many events from the subscription as are available and return them, raising a `TimeoutError` if the
        timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait for the event [s] before raising a `TimeoutError`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :return list((dict, dict)|(None, None)): a list of event-attributes pairs if the events are valid or `(None, None)` if they're invalid
        """
        pull_start_time = time.perf_counter()
        attempt = 1

        while self._alive:
            logger.debug("Pulling events from Google Pub/Sub: attempt %d.", attempt)

            pull_response = self.subscriber.pull(
                request={"subscription": self.subscription.path, "max_messages": MAX_SIMULTANEOUS_MESSAGES_PULL},
                retry=retry.Retry(),
            )

            if len(pull_response.received_messages) > 0:
                break
            else:
                logger.debug("Google Pub/Sub pull response timed out early.")
                attempt += 1

                pull_run_time = time.perf_counter() - pull_start_time

                if timeout is not None and pull_run_time > timeout:
                    raise TimeoutError(f"No message received from {self.subscription.topic!r} after {timeout} seconds.")

        if not pull_response.received_messages:
            return []

        self.subscriber.acknowledge(
            request={
                "subscription": self.subscription.path,
                "ack_ids": [message.ack_id for message in pull_response.received_messages],
            }
        )

        return [self._extract_and_validate_event(event) for event in pull_response.received_messages]

    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from a Pub/Sub message.

        :param dict container: a Pub/Sub message
        :return (any, dict): the event and its attributes
        """
        event = extract_event(container.message)
        attributes = get_nested_attribute(container.message, "attributes")
        attributes = ResponseAttributes.from_serialised_attributes(attributes)
        return event, attributes
