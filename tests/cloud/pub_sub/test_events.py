import datetime
import math
import uuid
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import MESSAGES, MockMessage, MockService, MockSubscription, MockTopic
from octue.cloud.emulators.child import ServicePatcher
from octue.cloud.events import OCTUE_SERVICES_PREFIX
from octue.cloud.pub_sub.events import GoogleCloudPubSubEventHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestPubSubEventHandler(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        cls.service_patcher.start()
        cls.question_uuid = str(uuid.uuid4())

        cls.topic = MockTopic(name=OCTUE_SERVICES_PREFIX, project_name=TEST_PROJECT_NAME)
        cls.topic.create(allow_existing=True)

        cls.subscription = MockSubscription(
            name=f"my-org.my-service.1-0-0.answers.{cls.question_uuid}",
            topic=cls.topic,
        )
        cls.subscription.create()

        cls.parent = MockService(
            service_id="my-org/my-service:1.0.0",
            backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME),
        )

    @classmethod
    def tearDownClass(cls):
        """Stop the services patcher.

        :return None:
        """
        cls.service_patcher.stop()

    def test_timeout(self):
        """Test that a TimeoutError is raised if message handling takes longer than the given timeout."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: message},
            schema={},
        )

        with self.assertRaises(TimeoutError):
            event_handler.handle_events(timeout=0)

    def test_in_order_messages_are_handled_in_order(self):
        """Test that messages received in order are handled in order."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
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

    def test_out_of_order_messages_are_handled_in_order(self):
        """Test that messages received out of order are handled in order."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
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
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
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
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
            )

        result = event_handler.handle_events(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            event_handler.handled_events,
            [{"kind": "test", "order": 0}, {"kind": "test", "order": 1}, {"kind": "finish-test", "order": 2}],
        )

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement message is handled correctly."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        messages = [
            {
                "event": {
                    "kind": "delivery_acknowledgement",
                    "datetime": datetime.datetime.utcnow().isoformat(),
                    "order": 0,
                },
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
            )

        result = event_handler.handle_events()
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_error_raised_if_heartbeat_not_received_before_checked(self):
        """Test that an error is raised if a heartbeat isn't received before a heartbeat is first checked for."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        # Check that the timeout is due to a heartbeat not being received.
        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_raised_if_heartbeats_stop_being_received(self):
        """Test that an error is raised if heartbeats stop being received within the maximum interval."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        event_handler._last_heartbeat = datetime.datetime.now() - datetime.timedelta(seconds=30)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_not_raised_if_heartbeat_has_been_received_in_maximum_allowed_interval(self):
        """Test that an error is not raised if a heartbeat has been received in the maximum allowed interval."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        event_handler._last_heartbeat = datetime.datetime.now()

        messages = [
            {
                "event": {
                    "kind": "delivery_acknowledgement",
                    "datetime": datetime.datetime.utcnow().isoformat(),
                    "order": 0,
                },
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
            )

        with patch(
            "octue.cloud.pub_sub.events.GoogleCloudPubSubEventHandler._time_since_last_heartbeat",
            datetime.timedelta(seconds=0),
        ):
            event_handler.handle_events(maximum_heartbeat_interval=0)

    def test_time_since_last_heartbeat_is_none_if_no_heartbeat_received_yet(self):
        """Test that the time since the last heartbeat is `None` if no heartbeat has been received yet."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        self.assertIsNone(event_handler._time_since_last_heartbeat)

    def test_total_run_time_is_none_if_handle_events_has_not_been_called(self):
        """Test that the total run time for the message handler is `None` if the `handle_events` method has not been
        called.
        """
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        self.assertIsNone(event_handler.total_run_time)

    def test_time_since_missing_message_is_none_if_no_unhandled_missing_messages(self):
        """Test that the `time_since_missing_message` property is `None` if there are no unhandled missing messages."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription, recipient=self.parent)
        self.assertIsNone(event_handler.time_since_missing_event)

    def test_missing_messages_at_start_can_be_skipped(self):
        """Test that missing messages at the start of the event stream can be skipped if they aren't received after a
        given time period if subsequent messages have been received.
        """
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
            skip_missing_events_after=0,
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Simulate the first two messages not being received.
        messages = [
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 3},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 4},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 5},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
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
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
            skip_missing_events_after=0,
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
            )

        # Send a final message.
        child.emit_event(
            event={"kind": "finish-test", "order": 5},
            attributes={"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            originator=self.parent.id,
            recipient=self.parent.id,
            # Simulate missing messages.
            order=5,
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
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
            skip_missing_events_after=0,
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                order=message["event"]["order"],
            )

        # Send another message.
        child.emit_event(
            event={"kind": "test", "order": 5},
            attributes={"order": 5, "question_uuid": self.question_uuid, "sender_type": "CHILD"},
            originator=self.parent.id,
            recipient=self.parent.id,
            # Simulate missing messages.
            order=5,
        )

        # Send more consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 20},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 21},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 22},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 23},
                "attributes": {"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child.emit_event(
                event=message["event"],
                attributes=message["attributes"],
                originator=self.parent.id,
                recipient=self.parent.id,
                # Simulate more missing messages.
                order=message["event"]["order"],
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
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
            skip_missing_events_after=0,
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send the result message.
        child.emit_event(
            event={"kind": "finish-test", "order": 1000},
            attributes={"question_uuid": self.question_uuid, "sender_type": "CHILD"},
            originator=self.parent.id,
            recipient=self.parent.id,
            # Simulate missing messages.
            order=1000,
        )

        event_handler.handle_events()

        # Check that the result message was handled.
        self.assertEqual(event_handler.handled_events, [{"kind": "finish-test", "order": 1000}])


class TestPullAndEnqueueAvailableMessages(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        cls.service_patcher.start()
        cls.question_uuid = str(uuid.uuid4())

        cls.topic = MockTopic(name=OCTUE_SERVICES_PREFIX, project_name=TEST_PROJECT_NAME)
        cls.topic.create(allow_existing=True)

        cls.subscription = MockSubscription(
            name=f"my-org.my-service.1-0-0.answers.{cls.question_uuid}",
            topic=cls.topic,
        )
        cls.subscription.create()

        cls.parent = MockService(
            service_id="my-org/my-service:1.0.0",
            backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME),
        )

    @classmethod
    def tearDownClass(cls):
        """Stop the services patcher.

        :return None:
        """
        cls.service_patcher.stop()

    def test_pull_and_enqueue_available_events(self):
        """Test that pulling and enqueuing a message works."""
        self.subscription = MockSubscription(
            name=f"my-org.my-service.1-0-0.answers.{self.question_uuid}",
            topic=self.topic,
        )

        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            schema={},
        )

        event_handler.question_uuid = self.question_uuid
        event_handler.child_sruid = "my-org/my-service:1.0.0"
        event_handler._child_sdk_version = "0.1.3"
        event_handler.waiting_events = {}

        # Enqueue a mock message for a mock subscription to receive.
        mock_message = {"kind": "test"}

        MESSAGES[self.question_uuid] = [
            MockMessage.from_primitive(
                mock_message,
                attributes={
                    "order": 0,
                    "question_uuid": self.question_uuid,
                    "originator": self.parent.id,
                    "sender": self.parent.id,
                    "sender_type": "CHILD",
                    "sender_sdk_version": "0.50.0",
                    "recipient": "my-org/my-service:1.0.0",
                },
            )
        ]

        event_handler._pull_and_enqueue_available_events(timeout=10)
        self.assertEqual(event_handler.waiting_events, {0: mock_message})
        self.assertEqual(event_handler._earliest_waiting_event_number, 0)

    def test_timeout_error_raised_if_result_message_not_received_in_time(self):
        """Test that a timeout error is raised if a result message is not received in time."""
        self.subscription = MockSubscription(
            name=f"my-org.my-service.1-0-0.answers.{self.question_uuid}",
            topic=self.topic,
        )

        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            recipient=self.parent,
            event_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
        )

        event_handler._child_sdk_version = "0.1.3"
        event_handler.waiting_events = {}
        event_handler._start_time = 0

        with self.assertRaises(TimeoutError):
            event_handler._pull_and_enqueue_available_events(timeout=1e-6)

        self.assertEqual(event_handler._earliest_waiting_event_number, math.inf)
