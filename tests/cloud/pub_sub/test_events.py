import datetime
import math
import uuid
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import (
    SUBSCRIPTIONS,
    MockMessage,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)
from octue.cloud.emulators.child import ServicePatcher
from octue.cloud.pub_sub.events import GoogleCloudPubSubEventHandler
from octue.cloud.service_id import split_service_id
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


parent = MockService(service_id="my-org/my-service:1.0.0", backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))


def create_mock_topic_and_subscription():
    """Create a question UUID, mock topic, and mock subscription.

    :return (str, octue.cloud.emulators._pub_sub.MockTopic, octue.cloud.emulators._pub_sub.MockSubscription): question UUID, topic, and subscription
    """
    question_uuid = str(uuid.uuid4())
    topic = MockTopic(name="my-org.my-service.1-0-0", project_name=TEST_PROJECT_NAME)
    subscription = MockSubscription(name=f"my-org.my-service.1-0-0.answers.{question_uuid}", topic=topic)

    subscription.create()
    return question_uuid, topic, subscription


class TestPubSubEventHandler(BaseTestCase):
    def test_timeout(self):
        """Test that a TimeoutError is raised if message handling takes longer than the given timeout."""
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: message},
                schema={},
            )

        with self.assertRaises(TimeoutError):
            event_handler.handle_events(timeout=0)

    def test_in_order_messages_are_handled_in_order(self):
        """Test that messages received in order are handled in order."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {"event": {"kind": "test"}, "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"}},
            {"event": {"kind": "test"}, "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"}},
            {"event": {"kind": "test"}, "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"}},
            {"event": {"kind": "finish-test"}, "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"}},
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events()
        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            event_handler.handled_events,
            [{"kind": "test"}, {"kind": "test"}, {"kind": "test"}, {"kind": "finish-test"}],
        )

    def test_out_of_order_messages_are_handled_in_order(self):
        """Test that messages received out of order are handled in order."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            mock_topic.messages_published = message["event"]["order"]
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events()

        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            event_handler.handled_events,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 3},
            ],
        )

    def test_out_of_order_messages_with_end_message_first_are_handled_in_order(self):
        """Test that messages received out of order and with the final message (the message that triggers a value to be
        returned) are handled in order.
        """
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            mock_topic.messages_published = message["event"]["order"]
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events()

        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            event_handler.handled_events,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 3},
            ],
        )

    def test_no_timeout(self):
        """Test that message handling works with no timeout."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            event_handler.handled_events,
            [{"kind": "test", "order": 0}, {"kind": "test", "order": 1}, {"kind": "finish-test", "order": 2}],
        )

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement message is handled correctly."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "delivery_acknowledgement", "datetime": datetime.datetime.utcnow().isoformat()},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result"},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events()
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_error_raised_if_heartbeat_not_received_before_checked(self):
        """Test that an error is raised if a heartbeat isn't received before a heartbeat is first checked for."""
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        # Check that the timeout is due to a heartbeat not being received.
        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_raised_if_heartbeats_stop_being_received(self):
        """Test that an error is raised if heartbeats stop being received within the maximum interval."""
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)

        event_handler._last_heartbeat = datetime.datetime.now() - datetime.timedelta(seconds=30)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_not_raised_if_heartbeat_has_been_received_in_maximum_allowed_interval(self):
        """Test that an error is not raised if a heartbeat has been received in the maximum allowed interval."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)

        event_handler._last_heartbeat = datetime.datetime.now()

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {
                    "kind": "delivery_acknowledgement",
                    "datetime": datetime.datetime.utcnow().isoformat(),
                },
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result"},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        with patch(
            "octue.cloud.pub_sub.events.GoogleCloudPubSubEventHandler._time_since_last_heartbeat",
            datetime.timedelta(seconds=0),
        ):
            event_handler.handle_events(maximum_heartbeat_interval=0)

    def test_time_since_last_heartbeat_is_none_if_no_heartbeat_received_yet(self):
        """Test that the time since the last heartbeat is `None` if no heartbeat has been received yet."""
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)

        self.assertIsNone(event_handler._time_since_last_heartbeat)

    def test_total_run_time_is_none_if_handle_events_has_not_been_called(self):
        """Test that the total run time for the message handler is `None` if the `handle_events` method has not been
        called.
        """
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()
        event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)
        self.assertIsNone(event_handler.total_run_time)

    def test_time_since_missing_message_is_none_if_no_unhandled_missing_messages(self):
        """Test that the `time_since_missing_message` property is `None` if there are no unhandled missing messages."""
        question_uuid, _, mock_subscription = create_mock_topic_and_subscription()
        event_handler = GoogleCloudPubSubEventHandler(subscription=mock_subscription, receiving_service=parent)
        self.assertIsNone(event_handler.time_since_missing_event)

    def test_missing_messages_at_start_can_be_skipped(self):
        """Test that missing messages at the start of the event stream can be skipped if they aren't received after a
        given time period if subsequent messages have been received.
        """
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_events_after=0,
            )

        # Simulate the first two messages not being received.
        mock_topic.messages_published = 2

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 3},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 4},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 5},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        result = event_handler.handle_events()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            event_handler.handled_events,
            [
                {"kind": "test", "order": 2},
                {"kind": "test", "order": 3},
                {"kind": "test", "order": 4},
                {"kind": "finish-test", "order": 5},
            ],
        )

    def test_missing_messages_in_middle_can_skipped(self):
        """Test that missing messages in the middle of the event stream can be skipped."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_events_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"],
                attributes=message["attributes"],
                topic=mock_topic,
                originator=parent.id,
            )

        # Simulate missing messages.
        mock_topic.messages_published = 5

        # Send a final message.
        child._send_message(
            message={"kind": "finish-test", "order": 5},
            attributes={"question_uuid": question_uuid, "sender_type": "CHILD"},
            topic=mock_topic,
            originator=parent.id,
        )

        event_handler.handle_events()

        # Check that all the non-missing messages were handled.
        self.assertEqual(
            event_handler.handled_events,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 5},
            ],
        )

    def test_multiple_blocks_of_missing_messages_in_middle_can_skipped(self):
        """Test that multiple blocks of missing messages in the middle of the event stream can be skipped."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_events_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"], attributes=message["attributes"], topic=mock_topic, originator=parent.id
            )

        # Simulate missing messages.
        mock_topic.messages_published = 5

        # Send another message.
        child._send_message(
            message={"kind": "test", "order": 5},
            attributes={"ordering_key": 5, "question_uuid": question_uuid, "sender_type": "CHILD"},
            topic=mock_topic,
            originator=parent.id,
        )

        # Simulate more missing messages.
        mock_topic.messages_published = 20

        # Send more consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 20},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 21},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 22},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 23},
                "attributes": {"question_uuid": question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(
                message=message["event"], attributes=message["attributes"], topic=mock_topic, originator=parent.id
            )

        event_handler.handle_events()

        # Check that all the non-missing messages were handled.
        self.assertEqual(
            event_handler.handled_events,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "test", "order": 5},
                {"kind": "test", "order": 20},
                {"kind": "test", "order": 21},
                {"kind": "test", "order": 22},
                {"kind": "finish-test", "order": 23},
            ],
        )

    def test_all_messages_missing_apart_from_result(self):
        """Test that the result message is still handled if all other messages are missing."""
        question_uuid, mock_topic, mock_subscription = create_mock_topic_and_subscription()

        with patch("octue.cloud.pub_sub.events.SubscriberClient", MockSubscriber):
            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_events_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Simulate missing messages.
        mock_topic.messages_published = 1000

        # Send the result message.
        child._send_message(
            message={"kind": "finish-test", "order": 1000},
            attributes={"question_uuid": question_uuid, "sender_type": "CHILD"},
            topic=mock_topic,
            originator=parent.id,
        )

        event_handler.handle_events()

        # Check that the result message was handled.
        self.assertEqual(event_handler.handled_events, [{"kind": "finish-test", "order": 1000}])


class TestPullAndEnqueueAvailableMessages(BaseTestCase):
    def test_pull_and_enqueue_available_events(self):
        """Test that pulling and enqueuing a message works."""
        question_uuid, mock_topic, _ = create_mock_topic_and_subscription()

        with ServicePatcher():
            mock_subscription = MockSubscription(
                name=f"my-org.my-service.1-0-0.answers.{question_uuid}",
                topic=mock_topic,
            )

            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

            event_handler.question_uuid = question_uuid
            event_handler.child_sruid = "my-org/my-service:1.0.0"
            event_handler._child_sdk_version = "0.1.3"
            event_handler.waiting_events = {}

            # Enqueue a mock message for a mock subscription to receive.
            mock_message = {"kind": "test"}

            originator_namespace, originator_name, originator_revision_tag = split_service_id(parent.id)

            SUBSCRIPTIONS[mock_subscription.name] = [
                MockMessage.from_primitive(
                    mock_message,
                    attributes={
                        "sender_type": "CHILD",
                        "ordering_key": 0,
                        "question_uuid": question_uuid,
                        "version": "0.50.0",
                        "originator_namespace": originator_namespace,
                        "originator_name": originator_name,
                        "originator_revision_tag": originator_revision_tag,
                        "sender_namespace": originator_namespace,
                        "sender_name": originator_name,
                        "sender_revision_tag": originator_revision_tag,
                        "recipient_namespace": "my-org",
                        "recipient_name": "my-service",
                        "recipient_revision_tag": "1.0.0",
                    },
                )
            ]

            event_handler._pull_and_enqueue_available_events(timeout=10)
            self.assertEqual(event_handler.waiting_events, {0: mock_message})
            self.assertEqual(event_handler._earliest_waiting_event_number, 0)

    def test_timeout_error_raised_if_result_message_not_received_in_time(self):
        """Test that a timeout error is raised if a result message is not received in time."""
        question_uuid, mock_topic, _ = create_mock_topic_and_subscription()

        with ServicePatcher():
            mock_subscription = MockSubscription(
                name=f"my-org.my-service.1-0-0.answers.{question_uuid}",
                topic=mock_topic,
            )

            event_handler = GoogleCloudPubSubEventHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

            event_handler._child_sdk_version = "0.1.3"
            event_handler.waiting_events = {}
            event_handler._start_time = 0

            # Create a mock subscription.
            SUBSCRIPTIONS[mock_subscription.name] = []

            with self.assertRaises(TimeoutError):
                event_handler._pull_and_enqueue_available_events(timeout=1e-6)

            self.assertEqual(event_handler._earliest_waiting_event_number, math.inf)
