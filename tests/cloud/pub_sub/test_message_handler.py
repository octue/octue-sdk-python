from unittest.mock import patch

from octue import exceptions
from octue.cloud.pub_sub.message_handler import OrderedMessageHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import (
    MockMessagePuller,
    MockPullResponse,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)


BACKEND = GCPPubSubBackend(
    project_name=TEST_PROJECT_NAME, credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS"
)


class TestOrderedMessageHandler(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the test class with a mock subscription.

        :return None:
        """
        service = MockService(backend=BACKEND)
        mock_topic = MockTopic(name="world", namespace="hello", service=service)
        cls.mock_subscription = MockSubscription(
            name="world",
            topic=mock_topic,
            namespace="hello",
            project_name=TEST_PROJECT_NAME,
            subscriber=MockSubscriber(),
        )

    def _make_order_recording_message_handler(self, message_handling_order):
        """Make a message handler that records the order in which messages were handled to the given list.

        :param list message_handling_order:
        :return callable:
        """

        def message_handler(message):
            message_handling_order.append(message["message_number"])

        return message_handler

    def test_timeout(self):
        """Test that a TimeoutError is raised if message handling takes longer than the given timeout."""
        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={
                "test": self._make_order_recording_message_handler([]),
                "finish-test": lambda message: message,
            },
        )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(messages=[{"type": "test", "message_number": 0}]).pull,
        ):
            with self.assertRaises(TimeoutError):
                message_handler.handle_messages(timeout=0)

    def test_unknown_message_type_raises_warning(self):
        """Test that unknown message types result in a warning being logged."""
        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={"finish-test": lambda message: message},
        )

        with patch("logging.StreamHandler.emit") as mock_emit:
            message_handler._handle_message({"type": "blah", "message_number": 0})

        self.assertIn("Received a message of unknown type", mock_emit.call_args_list[0][0][0].msg)

    def test_in_order_messages_are_handled_in_order(self):
        """Test that messages received in order are handled in order."""
        message_handling_order = []

        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={
                "test": self._make_order_recording_message_handler(message_handling_order),
                "finish-test": lambda message: "This is the result.",
            },
        )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(
                messages=[
                    {"type": "test", "message_number": 0},
                    {"type": "test", "message_number": 1},
                    {"type": "test", "message_number": 2},
                    {"type": "finish-test", "message_number": 3},
                ]
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handling_order, [0, 1, 2])

    def test_out_of_order_messages_are_handled_in_order(self):
        """Test that messages received out of order are handled in order."""
        message_handling_order = []

        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={
                "test": self._make_order_recording_message_handler(message_handling_order),
                "finish-test": lambda message: "This is the result.",
            },
        )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(
                messages=[
                    {"type": "test", "message_number": 1},
                    {"type": "test", "message_number": 2},
                    {"type": "test", "message_number": 0},
                    {"type": "finish-test", "message_number": 3},
                ]
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handling_order, [0, 1, 2])

    def test_out_of_order_messages_with_end_message_first_are_handled_in_order(self):
        """Test that messages received out of order and with the final message (the message that triggers a value to be
        returned) are handled in order.
        """
        message_handling_order = []

        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={
                "test": self._make_order_recording_message_handler(message_handling_order),
                "finish-test": lambda message: "This is the result.",
            },
        )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(
                messages=[
                    {"type": "finish-test", "message_number": 3},
                    {"type": "test", "message_number": 1},
                    {"type": "test", "message_number": 2},
                    {"type": "test", "message_number": 0},
                ]
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handling_order, [0, 1, 2])

    def test_no_timeout(self):
        """Test that message handling works with no timeout."""
        message_handling_order = []

        message_handler = OrderedMessageHandler(
            subscriber=MockSubscriber(),
            subscription=self.mock_subscription,
            message_handlers={
                "test": self._make_order_recording_message_handler(message_handling_order),
                "finish-test": lambda message: "This is the result.",
            },
        )

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(
                messages=[
                    {"type": "finish-test", "message_number": 2},
                    {"type": "test", "message_number": 0},
                    {"type": "test", "message_number": 1},
                ]
            ).pull,
        ):
            result = message_handler.handle_messages(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(message_handling_order, [0, 1])

    def test_error_raised_if_delivery_acknowledgement_not_received_in_time(self):
        """Test that an error is raised if delivery acknowledgement isn't received before the given acknowledgement
        timeout.
        """
        message_handler = OrderedMessageHandler(subscriber=MockSubscriber(), subscription=self.mock_subscription)

        with patch("tests.cloud.pub_sub.mocks.MockSubscriber.pull", return_value=MockPullResponse()):
            with self.assertRaises(exceptions.QuestionNotDelivered):
                message_handler.handle_messages(delivery_acknowledgement_timeout=0)

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement message is handled correctly."""
        message_handler = OrderedMessageHandler(subscriber=MockSubscriber(), subscription=self.mock_subscription)
        self.assertFalse(message_handler.received_delivery_acknowledgement)

        with patch(
            "octue.cloud.pub_sub.service.OrderedMessageHandler._pull_message",
            new=MockMessagePuller(
                [
                    {
                        "type": "delivery_acknowledgement",
                        "delivery_time": "2021-11-17 17:33:59.717428",
                        "message_number": 0,
                    },
                    {"type": "result", "output_values": None, "output_manifest": None, "message_number": 1},
                ]
            ).pull,
        ):
            result = message_handler.handle_messages()

        self.assertTrue(message_handler.received_delivery_acknowledgement)
        self.assertEqual(result, {"output_values": None, "output_manifest": None})
