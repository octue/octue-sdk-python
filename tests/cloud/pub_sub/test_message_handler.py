import datetime
import json
import math
from unittest.mock import patch

from octue import exceptions
from octue.cloud.emulators._pub_sub import (
    MESSAGES,
    MockMessage,
    MockMessagePuller,
    MockPullResponse,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)
from octue.cloud.emulators.child import ServicePatcher
from octue.cloud.pub_sub.message_handler import OrderedMessageHandler
from octue.exceptions import QuestionNotDelivered
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


mock_topic = MockTopic(name="world", project_name=TEST_PROJECT_NAME)
receiving_service = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
mock_subscription = MockSubscription(name="world", topic=mock_topic, project_name=TEST_PROJECT_NAME)


class TestOrderedMessageHandler(BaseTestCase):
    def test_timeout(self):
        """Test that a TimeoutError is raised if message handling takes longer than the given timeout."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: message},
            )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(
                messages=[{"type": "test", "message_number": 0}],
                message_handler=message_handler,
            ).pull,
        ):
            with self.assertRaises(TimeoutError):
                message_handler.handle_messages(timeout=0)

    def test_unknown_message_type_raises_warning(self):
        """Test that unknown message types result in a warning being logged."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"finish-test": lambda message: message},
            )

        with self.assertLogs() as logging_context:
            message_handler._handle_message({"type": "blah", "message_number": 0})

        self.assertIn("received a message of unknown type", logging_context.output[1])

    def test_in_order_messages_are_handled_in_order(self):
        """Test that messages received in order are handled in order."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        messages = [
            {"type": "test", "message_number": 0},
            {"type": "test", "message_number": 1},
            {"type": "test", "message_number": 2},
            {"type": "finish-test", "message_number": 3},
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handler.handled_messages, messages)

    def test_out_of_order_messages_are_handled_in_order(self):
        """Test that messages received out of order are handled in order."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        messages = [
            {"type": "test", "message_number": 1},
            {"type": "test", "message_number": 2},
            {"type": "test", "message_number": 0},
            {"type": "finish-test", "message_number": 3},
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            message_handler.handled_messages,
            [
                {"type": "test", "message_number": 0},
                {"type": "test", "message_number": 1},
                {"type": "test", "message_number": 2},
                {"type": "finish-test", "message_number": 3},
            ],
        )

    def test_out_of_order_messages_with_end_message_first_are_handled_in_order(self):
        """Test that messages received out of order and with the final message (the message that triggers a value to be
        returned) are handled in order.
        """
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(
                messages=[
                    {"type": "finish-test", "message_number": 3},
                    {"type": "test", "message_number": 1},
                    {"type": "test", "message_number": 2},
                    {"type": "test", "message_number": 0},
                ],
                message_handler=message_handler,
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            message_handler.handled_messages,
            [
                {"type": "test", "message_number": 0},
                {"type": "test", "message_number": 1},
                {"type": "test", "message_number": 2},
                {"type": "finish-test", "message_number": 3},
            ],
        )

    def test_no_timeout(self):
        """Test that message handling works with no timeout."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        messages = [
            {"type": "test", "message_number": 0},
            {"type": "test", "message_number": 1},
            {"type": "finish-test", "message_number": 2},
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handler.handled_messages, messages)

    def test_error_raised_if_delivery_acknowledgement_not_received_in_time(self):
        """Test that an error is raised if delivery acknowledgement isn't received before the given acknowledgement
        timeout.
        """
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
            )

        with patch("octue.cloud.emulators._pub_sub.MockSubscriber.pull", return_value=MockPullResponse()):
            with self.assertRaises(exceptions.QuestionNotDelivered):
                message_handler.handle_messages(delivery_acknowledgement_timeout=0)

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement message is handled correctly."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
            )

        self.assertFalse(message_handler.received_response_from_child)

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(
                [
                    {
                        "type": "delivery_acknowledgement",
                        "delivery_time": "2021-11-17 17:33:59.717428",
                        "message_number": 0,
                    },
                    {"type": "result", "output_values": None, "output_manifest": None, "message_number": 1},
                ],
                message_handler=message_handler,
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertTrue(message_handler.received_response_from_child)
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_delivery_acknowledgement_if_delivery_acknowledgement_message_missed(self):
        """Test that delivery of a question is acknowledged by receipt of another message type if the delivery
        acknowledgement message is missed.
        """
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
            )

        self.assertFalse(message_handler.received_response_from_child)

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(
                [
                    {"type": "monitor_message", "data": "splat", "message_number": 0},
                    {"type": "result", "output_values": None, "output_manifest": None, "message_number": 1},
                ],
                message_handler=message_handler,
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertTrue(message_handler.received_response_from_child)
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_error_raised_if_heartbeat_not_received_before_checked(self):
        """Test that an error is raised if a heartbeat isn't received before a heartbeat is first checked for."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
            )

        with self.assertRaises(TimeoutError) as error:
            message_handler.handle_messages(maximum_heartbeat_interval=0)

        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_raised_if_heartbeats_stop_being_received(self):
        """Test that an error is raised if heartbeats stop being received within the maximum interval."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
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
                receiving_service=receiving_service,
            )

        message_handler._last_heartbeat = datetime.datetime.now()

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(
                messages=[
                    {
                        "type": "delivery_acknowledgement",
                        "delivery_time": "2021-11-17 17:33:59.717428",
                        "message_number": 0,
                    },
                    {"type": "result", "output_values": None, "output_manifest": None, "message_number": 1},
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
                receiving_service=receiving_service,
            )

        self.assertIsNone(message_handler._time_since_last_heartbeat)

    def test_total_run_time_is_none_if_handle_messages_has_not_been_called(self):
        """Test that the total run time for the message handler is `None` if the `handle_messages` method has not been
        called.
        """
        message_handler = OrderedMessageHandler(
            subscription=mock_subscription,
            receiving_service=receiving_service,
        )

        self.assertIsNone(message_handler.total_run_time)

    def test_handler_can_skip_first_n_messages_if_missed(self):
        """Test that the first n messages can be skipped if they aren't received after a given time period if subsequent
        messages have been received.
        """
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        # Simulate the first two messages not being received.
        message_handler._earliest_message_number_received = 2

        messages = [
            {"type": "test", "message_number": 2},
            {"type": "test", "message_number": 3},
            {"type": "test", "message_number": 4},
            {"type": "finish-test", "message_number": 5},
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            result = message_handler.handle_messages(skip_first_messages_after=0)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handler.handled_messages, messages)

    def test_later_missing_messages_cannot_be_skipped(self):
        """Test that missing messages that aren't in the first n messages cannot be skipped."""
        with patch("octue.cloud.pub_sub.message_handler.SubscriberClient", MockSubscriber):
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

        messages = [
            {"type": "test", "message_number": 0},
            {"type": "test", "message_number": 1},
            {"type": "test", "message_number": 2},
            {"type": "finish-test", "message_number": 5},
        ]

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_and_enqueue_message",
            new=MockMessagePuller(messages=messages, message_handler=message_handler).pull,
        ):
            with self.assertRaises(TimeoutError):
                message_handler.handle_messages(timeout=0.5, skip_first_messages_after=0)

        # Check that only the first three messages were handled.
        self.assertEqual(message_handler.handled_messages, messages[:3])


class TestPullAndEnqueueMessage(BaseTestCase):
    def test_pull_and_enqueue_message(self):
        """Test that pulling and enqueuing a message works."""
        with ServicePatcher():
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

            message_handler._child_sdk_version = "0.1.3"
            message_handler._waiting_messages = {}

            # Create a mock topic and publish a mock message to it.
            mock_message = {"type": "test", "message_number": 0}
            MESSAGES["octue.services.world"] = []
            MESSAGES["octue.services.world"].append(MockMessage(data=json.dumps(mock_message).encode()))

            message_handler._pull_and_enqueue_message(timeout=1000, delivery_acknowledgement_timeout=1000)
            self.assertEqual(message_handler._waiting_messages, {0: mock_message})
            self.assertEqual(message_handler._earliest_message_number_received, 0)

    def test_timeout_error_raised_if_result_not_received_in_time(self):
        """Test that a timeout error is raised if a result message is not received in time."""
        with ServicePatcher():
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

            message_handler._child_sdk_version = "0.1.3"
            message_handler._waiting_messages = {}
            message_handler._start_time = 0

            # Create a mock topic and publish a mock non-result message to it.
            mock_message = {"type": "test", "message_number": 0}
            MESSAGES["octue.services.world"] = []
            MESSAGES["octue.services.world"].append(MockMessage(data=json.dumps(mock_message).encode()))
            message_handler._pull_and_enqueue_message(timeout=0.1, delivery_acknowledgement_timeout=100)

            with self.assertRaises(TimeoutError):
                message_handler._pull_and_enqueue_message(timeout=0.1, delivery_acknowledgement_timeout=100)

            self.assertEqual(message_handler._waiting_messages, {0: mock_message})
            self.assertEqual(message_handler._earliest_message_number_received, 0)

    def test_question_not_delivered_error_raised_if_no_message_received_in_time(self):
        """Test that a question not delivered error is raised if no messages are received at all within the delivery
        acknowledgement deadline.
        """
        with ServicePatcher():
            message_handler = OrderedMessageHandler(
                subscription=mock_subscription,
                receiving_service=receiving_service,
                message_handlers={"test": lambda message: None, "finish-test": lambda message: "This is the result."},
            )

            message_handler._child_sdk_version = "0.1.3"
            message_handler._waiting_messages = {}
            message_handler._start_time = 0

            # Create a mock topic.
            MESSAGES["octue.services.world"] = []

            with self.assertRaises(QuestionNotDelivered):
                message_handler._pull_and_enqueue_message(timeout=100, delivery_acknowledgement_timeout=0.1)

            self.assertEqual(message_handler._waiting_messages, {})
            self.assertEqual(message_handler._earliest_message_number_received, math.inf)
