import datetime
import json
import math
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import (
    SUBSCRIPTIONS,
    MockMessage,
    MockMessagePuller,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)
from octue.cloud.emulators.child import ServicePatcher
from octue.cloud.pub_sub.message_handler import OrderedMessageHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


QUESTION_UUID = "1c766d5e-4c6c-456a-b1af-17c7f453999d"


mock_topic = MockTopic(name="my-org.my-service.1-0-0", project_name=TEST_PROJECT_NAME)

mock_subscription = MockSubscription(
    name=f"my-org.my-service.1-0-0.answers.{QUESTION_UUID}",
    topic=mock_topic,
    project_name=TEST_PROJECT_NAME,
)
mock_subscription.create()

parent = MockService(service_id="my-org/my-service:1.0.0", backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))


class TestOrderedMessageHandler(BaseTestCase):
    def test_timeout(self):
        """Test that a TimeoutError is raised if message handling takes longer than the given timeout."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: message},
                schema={},
            )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(
                messages=[MockMessage(b"")],
                message_handler=message_handler,
            ).pull,
        ):
            with self.assertRaises(TimeoutError):
                message_handler.handle_messages(timeout=0)

    def test_in_order_messages_are_handled_in_order(self):
        """Test that messages received in order are handled in order."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        messages = [
            MockMessage.from_primitive({"kind": "test"}, attributes={"message_number": 0}),
            MockMessage.from_primitive({"kind": "test"}, attributes={"message_number": 1}),
            MockMessage.from_primitive({"kind": "test"}, attributes={"message_number": 2}),
            MockMessage.from_primitive({"kind": "finish-test"}, attributes={"message_number": 3}),
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handler.handled_messages, [json.loads(message.data.decode()) for message in messages])

    def test_out_of_order_messages_are_handled_in_order(self):
        """Test that messages received out of order are handled in order."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        messages = [
            MockMessage.from_primitive({"kind": "test", "order": 1}, attributes={"message_number": 1}),
            MockMessage.from_primitive({"kind": "test", "order": 2}, attributes={"message_number": 2}),
            MockMessage.from_primitive({"kind": "test", "order": 0}, attributes={"message_number": 0}),
            MockMessage.from_primitive({"kind": "finish-test", "order": 3}, attributes={"message_number": 3}),
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            message_handler.handled_messages,
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
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(
                messages=[
                    MockMessage.from_primitive({"kind": "finish-test", "order": 3}, attributes={"message_number": 3}),
                    MockMessage.from_primitive({"kind": "test", "order": 1}, attributes={"message_number": 1}),
                    MockMessage.from_primitive({"kind": "test", "order": 2}, attributes={"message_number": 2}),
                    MockMessage.from_primitive({"kind": "test", "order": 0}, attributes={"message_number": 0}),
                ],
                message_handler=message_handler,
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            message_handler.handled_messages,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 3},
            ],
        )

    def test_no_timeout(self):
        """Test that message handling works with no timeout."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

        messages = [
            MockMessage.from_primitive({"kind": "test", "order": 0}, attributes={"message_number": 0}),
            MockMessage.from_primitive({"kind": "test", "order": 1}, attributes={"message_number": 1}),
            MockMessage.from_primitive({"kind": "finish-test", "order": 2}, attributes={"message_number": 2}),
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            message_handler.handled_messages,
            [{"kind": "test", "order": 0}, {"kind": "test", "order": 1}, {"kind": "finish-test", "order": 2}],
        )

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement message is handled correctly."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
            )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(
                [
                    MockMessage.from_primitive(
                        {
                            "kind": "delivery_acknowledgement",
                            "datetime": "2021-11-17 17:33:59.717428",
                        },
                        attributes={"message_number": 0},
                    ),
                    MockMessage.from_primitive(
                        {"kind": "result", "output_values": None, "output_manifest": None},
                        attributes={"message_number": 1},
                    ),
                ],
                message_handler=message_handler,
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_error_raised_if_heartbeat_not_received_before_checked(self):
        """Test that an error is raised if a heartbeat isn't received before a heartbeat is first checked for."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
            )

        with patch("octue.cloud.pub_sub.message_handler.OrderedMessageHandler._pull_and_enqueue_available_messages"):
            with self.assertRaises(TimeoutError) as error:
                message_handler.handle_messages(maximum_heartbeat_interval=0)

        # Check that the timeout is due to a heartbeat not being received.
        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_raised_if_heartbeats_stop_being_received(self):
        """Test that an error is raised if heartbeats stop being received within the maximum interval."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
            )

        message_handler._last_heartbeat = datetime.datetime.now() - datetime.timedelta(seconds=30)

        with self.assertRaises(TimeoutError) as error:
            message_handler.handle_messages(maximum_heartbeat_interval=0)

        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_not_raised_if_heartbeat_has_been_received_in_maximum_allowed_interval(self):
        """Test that an error is not raised if a heartbeat has been received in the maximum allowed interval."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
            )

        message_handler._last_heartbeat = datetime.datetime.now()

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(
                messages=[
                    MockMessage.from_primitive(
                        {
                            "kind": "delivery_acknowledgement",
                            "datetime": "2021-11-17 17:33:59.717428",
                        },
                        attributes={"message_number": 0},
                    ),
                    MockMessage.from_primitive(
                        {"kind": "result", "output_values": None, "output_manifest": None},
                        attributes={"message_number": 1},
                    ),
                ],
                message_handler=message_handler,
            ).pull,
        ):
            with patch(
                "octue.cloud.pub_sub.message_handler.OrderedMessageHandler._time_since_last_heartbeat",
                datetime.timedelta(seconds=0),
            ):
                message_handler.handle_messages(maximum_heartbeat_interval=0)

    def test_time_since_last_heartbeat_is_none_if_no_heartbeat_received_yet(self):
        """Test that the time since the last heartbeat is `None` if no heartbeat has been received yet."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
            )

        self.assertIsNone(message_handler._time_since_last_heartbeat)

    def test_total_run_time_is_none_if_handle_messages_has_not_been_called(self):
        """Test that the total run time for the message handler is `None` if the `handle_messages` method has not been
        called.
        """
        message_handler = OrderedMessageHandler(
            subscription=mock_subscription,
            receiving_service=parent,
        )

        self.assertIsNone(message_handler.total_run_time)

    def test_time_since_missing_message_is_none_if_no_missing_messages(self):
        message_handler = OrderedMessageHandler(
            subscription=mock_subscription,
            receiving_service=parent,
        )

        self.assertIsNone(message_handler.time_since_missing_message)

    def test_missing_messages_at_start_can_be_skipped(self):
        """Test that the first n messages can be skipped if they aren't received after a given time period if subsequent
        messages have been received.
        """
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_messages_after=0,
            )

        # Simulate the first two messages not being received.
        message_handler._earliest_waiting_message_number = 2

        messages = [
            MockMessage.from_primitive({"kind": "test", "order": 2}, attributes={"message_number": 2}),
            MockMessage.from_primitive({"kind": "test", "order": 3}, attributes={"message_number": 3}),
            MockMessage.from_primitive({"kind": "test", "order": 4}, attributes={"message_number": 4}),
            MockMessage.from_primitive({"kind": "finish-test", "order": 5}, attributes={"message_number": 5}),
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_available_messages",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            message_handler.handled_messages,
            [
                {"kind": "test", "order": 2},
                {"kind": "test", "order": 3},
                {"kind": "test", "order": 4},
                {"kind": "finish-test", "order": 5},
            ],
        )

    def test_missing_messages_in_middle_can_skipped(self):
        """Test that missing messages in the middle of the event stream can be skipped."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_messages_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(message=message["event"], attributes=message["attributes"], topic=mock_topic)

        # Simulate missing messages.
        mock_topic.messages_published = 5

        # Send a final message.
        child._send_message(
            message={"kind": "finish-test", "order": 5},
            attributes={"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            topic=mock_topic,
        )

        message_handler.handle_messages()

        # Check that all the non-missing messages were handled.
        self.assertEqual(
            message_handler.handled_messages,
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 5},
            ],
        )

    def test_multiple_blocks_of_missing_messages_in_middle_can_skipped(self):
        """Test that multiple blocks of missing messages in the middle of the event stream can be skipped."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_messages_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Send three consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(message=message["event"], attributes=message["attributes"], topic=mock_topic)

        # Simulate missing messages.
        mock_topic.messages_published = 5

        # Send another message.
        child._send_message(
            message={"kind": "test", "order": 5},
            attributes={"message_number": 5, "question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            topic=mock_topic,
        )

        # Simulate more missing messages.
        mock_topic.messages_published = 20

        # Send more consecutive messages.
        messages = [
            {
                "event": {"kind": "test", "order": 20},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 21},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 22},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 23},
                "attributes": {"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            },
        ]

        for message in messages:
            child._send_message(message=message["event"], attributes=message["attributes"], topic=mock_topic)

        message_handler.handle_messages()

        # Check that all the non-missing messages were handled.
        self.assertEqual(
            message_handler.handled_messages,
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
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
                skip_missing_messages_after=0,
            )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        # Simulate missing messages.
        mock_topic.messages_published = 1000

        # Send the result message.
        child._send_message(
            message={"kind": "finish-test", "order": 1000},
            attributes={"question_uuid": QUESTION_UUID, "sender_type": "CHILD"},
            topic=mock_topic,
        )

        message_handler.handle_messages()

        # Check that the result message was handled.
        self.assertEqual(message_handler.handled_messages, [{"kind": "finish-test", "order": 1000}])


class TestPullAndEnqueueMessage(BaseTestCase):
    def test_pull_and_enqueue_available_messages(self):
        """Test that pulling and enqueuing a message works."""
        question_uuid = "4d31bb46-66c4-4e68-831f-e51e17e651ef"

        with ServicePatcher():
            mock_subscription = MockSubscription(
                name=f"my-org.my-service.1-0-0.answers.{question_uuid}",
                topic=mock_topic,
                project_name=TEST_PROJECT_NAME,
            )

            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
                schema={},
            )

            message_handler._child_sdk_version = "0.1.3"
            message_handler.waiting_messages = {}

            # Enqueue a mock message for a mock subscription to receive.
            mock_message = {"kind": "test"}

            SUBSCRIPTIONS[mock_subscription.name] = [
                MockMessage.from_primitive(
                    mock_message,
                    attributes={
                        "sender_type": "CHILD",
                        "message_number": 0,
                        "question_uuid": question_uuid,
                        "version": "0.50.0",
                    },
                )
            ]

            message_handler._pull_and_enqueue_available_messages(timeout=10)
            self.assertEqual(message_handler.waiting_messages, {0: mock_message})
            self.assertEqual(message_handler._earliest_waiting_message_number, 0)

    def test_timeout_error_raised_if_result_message_not_received_in_time(self):
        """Test that a timeout error is raised if a result message is not received in time."""
        question_uuid = "4d31bb46-66c4-4e68-831f-e51e17e651ef"

        with ServicePatcher():
            mock_subscription = MockSubscription(
                name=f"my-org.my-service.1-0-0.answers.{question_uuid}",
                topic=mock_topic,
                project_name=TEST_PROJECT_NAME,
            )

            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=parent,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

            message_handler._child_sdk_version = "0.1.3"
            message_handler.waiting_messages = {}
            message_handler._start_time = 0

            # Create a mock subscription.
            SUBSCRIPTIONS[mock_subscription.name] = []

            with self.assertRaises(TimeoutError):
                message_handler._pull_and_enqueue_available_messages(timeout=1e-6)

            self.assertEqual(message_handler._earliest_waiting_message_number, math.inf)
