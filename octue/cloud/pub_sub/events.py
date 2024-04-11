import base64
import json
import logging
import time
from datetime import datetime, timedelta

from google.api_core import retry
from google.cloud.pubsub_v1 import SubscriberClient

from octue.cloud.events.handler import AbstractEventHandler
from octue.cloud.events.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.utils.decoders import OctueJSONDecoder
from octue.utils.objects import getattr_or_subscribe
from octue.utils.threads import RepeatingTimer


logger = logging.getLogger(__name__)

MAX_SIMULTANEOUS_MESSAGES_PULL = 50


def extract_event_and_attributes_from_pub_sub_message(message):
    """Extract an Octue service event and its attributes from a Google Pub/Sub message in either direct Pub/Sub format
    or in the Google Cloud Run format.

    :param dict|google.cloud.pubsub_v1.subscriber.message.Message message: the message in Google Cloud Run format or Google Pub/Sub format
    :return (any, dict): the extracted event and its attributes
    """
    # Cast attributes to a dictionary to avoid defaultdict-like behaviour from Pub/Sub message attributes container.
    attributes = dict(getattr_or_subscribe(message, "attributes"))

    # Deserialise the `order` and `forward_logs` fields if they're present (don't assume they are before validation).
    if attributes.get("order"):
        attributes["order"] = int(attributes["order"])

    # Required for question events.
    if attributes.get("sender_type") == "PARENT":
        forward_logs = attributes.get("forward_logs")

        if forward_logs:
            attributes["forward_logs"] = bool(int(forward_logs))
        else:
            attributes["forward_logs"] = None

    try:
        # Parse event directly from Pub/Sub or Dataflow.
        event = json.loads(message.data.decode(), cls=OctueJSONDecoder)
    except Exception:
        # Parse event from Google Cloud Run.
        event = json.loads(base64.b64decode(message["data"]).decode("utf-8").strip(), cls=OctueJSONDecoder)

    return event, attributes


class GoogleCloudPubSubEventHandler(AbstractEventHandler):
    """A synchronous handler for events received as Google Pub/Sub messages from a pull subscription.

    :param octue.cloud.pub_sub.subscription.Subscription subscription: the subscription messages are pulled from
    :param octue.cloud.pub_sub.service.Service recipient: the `Service` instance that's receiving the events
    :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive
    :param bool record_events: if `True`, record received events in the `received_events` attribute
    :param dict|None event_handlers: a mapping of event type names to callables that handle each type of event. The handlers must not mutate the events.
    :param dict|str schema: the JSON schema to validate events against
    :param int|float skip_missing_events_after: the number of seconds after which to skip any events if they haven't arrived but subsequent events have
    :return None:
    """

    def __init__(
        self,
        subscription,
        recipient,
        handle_monitor_message=None,
        record_events=True,
        event_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
        skip_missing_events_after=10,
    ):
        self.subscription = subscription

        super().__init__(
            recipient,
            handle_monitor_message=handle_monitor_message,
            record_events=record_events,
            event_handlers=event_handlers,
            schema=schema,
            skip_missing_events_after=skip_missing_events_after,
        )

        self._subscriber = SubscriberClient()
        self._heartbeat_checker = None
        self._last_heartbeat = None
        self._alive = True

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

    def handle_events(self, timeout=60, maximum_heartbeat_interval=300):
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
                self._pull_and_enqueue_available_events(timeout=pull_timeout)
                result = self._attempt_to_handle_waiting_events()

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

    def _pull_and_enqueue_available_events(self, timeout):
        """Pull as many events from the subscription as are available and enqueue them in `self.waiting_events`,
        raising a `TimeoutError` if the timeout is exceeded before succeeding.

        :param float|None timeout: how long to wait for the event [s] before raising a `TimeoutError`
        :raise TimeoutError|concurrent.futures.TimeoutError: if the timeout is exceeded
        :return None:
        """
        pull_start_time = time.perf_counter()
        attempt = 1

        while self._alive:
            logger.debug("Pulling events from Google Pub/Sub: attempt %d.", attempt)

            pull_response = self._subscriber.pull(
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
            return

        self._subscriber.acknowledge(
            request={
                "subscription": self.subscription.path,
                "ack_ids": [message.ack_id for message in pull_response.received_messages],
            }
        )

        for event in pull_response.received_messages:
            self._extract_and_enqueue_event(event)

    def _extract_event_and_attributes(self, container):
        """Extract an event and its attributes from a Pub/Sub message.

        :param dict container: a Pub/Sub message
        :return (any, dict): the event and its attributes
        """
        return extract_event_and_attributes_from_pub_sub_message(container.message)
