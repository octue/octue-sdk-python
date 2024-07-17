import datetime
import os
import uuid
from unittest.mock import patch

from octue.cloud.emulators._pub_sub import MESSAGES, MockMessage, MockService, MockSubscription
from octue.cloud.emulators.service import ServicePatcher
from octue.cloud.pub_sub.events import GoogleCloudPubSubEventHandler
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestGoogleCloudPubSubEventHandler(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        cls.service_patcher.start()
        cls.question_uuid = str(uuid.uuid4())

        cls.subscription = MockSubscription(
            name=f"octue.services.my-org.my-service.1-0-0.answers.{cls.question_uuid}",
            topic=cls.test_result_modifier.services_topic,
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

    def test_instantiating_without_credentials(self):
        """Test that the event handler can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            GoogleCloudPubSubEventHandler(subscription=self.subscription)

    def test_timeout(self):
        """Test that a TimeoutError is raised if event handling takes longer than the given timeout."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            event_handlers={"test": lambda event, attributes: None, "finish-test": lambda event, attributes: event},
            schema={},
        )

        with self.assertRaises(TimeoutError):
            event_handler.handle_events(timeout=0)

    def test_handle_events(self):
        """Test events can be handled."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            event_handlers={
                "test": lambda event, attributes: None,
                "finish-test": lambda event, attributes: "This is the result.",
            },
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        events = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 2},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 3},
                "attributes": {"sender_type": "CHILD"},
            },
        ]

        for event in events:
            child._emit_event(
                event=event["event"],
                question_uuid=self.question_uuid,
                parent_question_uuid=None,
                originator_question_uuid=self.question_uuid,
                attributes=event["attributes"],
                parent=self.parent.id,
                originator=self.parent.id,
                recipient=self.parent.id,
                retry_count=0,
            )

        result = event_handler.handle_events()
        self.assertEqual(result, "This is the result.")

        self.assertEqual(
            [event["event"] for event in event_handler.handled_events],
            [
                {"kind": "test", "order": 0},
                {"kind": "test", "order": 1},
                {"kind": "test", "order": 2},
                {"kind": "finish-test", "order": 3},
            ],
        )

    def test_no_timeout(self):
        """Test that event handling works with no timeout."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            event_handlers={
                "test": lambda event, attributes: None,
                "finish-test": lambda event, attributes: "This is the result.",
            },
            schema={},
        )

        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        events = [
            {
                "event": {"kind": "test", "order": 0},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "test", "order": 1},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "finish-test", "order": 2},
                "attributes": {"sender_type": "CHILD"},
            },
        ]

        for event in events:
            child._emit_event(
                event=event["event"],
                question_uuid=self.question_uuid,
                parent_question_uuid=None,
                originator_question_uuid=self.question_uuid,
                attributes=event["attributes"],
                parent=self.parent.id,
                originator=self.parent.id,
                recipient=self.parent.id,
                retry_count=0,
            )

        result = event_handler.handle_events(timeout=None)

        self.assertEqual(result, "This is the result.")
        self.assertEqual(
            [event["event"] for event in event_handler.handled_events],
            [{"kind": "test", "order": 0}, {"kind": "test", "order": 1}, {"kind": "finish-test", "order": 2}],
        )

    def test_delivery_acknowledgement(self):
        """Test that a delivery acknowledgement event is handled correctly."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)
        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))

        events = [
            {
                "event": {"kind": "delivery_acknowledgement", "order": 0},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result", "order": 1},
                "attributes": {"sender_type": "CHILD"},
            },
        ]

        for event in events:
            child._emit_event(
                event=event["event"],
                question_uuid=self.question_uuid,
                parent_question_uuid=None,
                originator_question_uuid=self.question_uuid,
                attributes=event["attributes"],
                parent=self.parent.id,
                originator=self.parent.id,
                recipient=self.parent.id,
                retry_count=0,
            )

        result = event_handler.handle_events()
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_error_raised_if_heartbeat_not_received_before_checked(self):
        """Test that an error is raised if a heartbeat isn't received before a heartbeat is first checked for."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        # Check that the timeout is due to a heartbeat not being received.
        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_raised_if_heartbeats_stop_being_received(self):
        """Test that an error is raised if heartbeats stop being received within the maximum interval."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)
        event_handler._last_heartbeat = datetime.datetime.now() - datetime.timedelta(seconds=30)

        with self.assertRaises(TimeoutError) as error:
            event_handler.handle_events(maximum_heartbeat_interval=0)

        self.assertIn("heartbeat", error.exception.args[0])

    def test_error_not_raised_if_heartbeat_has_been_received_in_maximum_allowed_interval(self):
        """Test that an error is not raised if a heartbeat has been received in the maximum allowed interval."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)
        child = MockService(backend=GCPPubSubBackend(project_name=TEST_PROJECT_NAME))
        event_handler._last_heartbeat = datetime.datetime.now()

        events = [
            {
                "event": {"kind": "delivery_acknowledgement", "order": 0},
                "attributes": {"sender_type": "CHILD"},
            },
            {
                "event": {"kind": "result", "order": 1},
                "attributes": {"sender_type": "CHILD"},
            },
        ]

        for event in events:
            child._emit_event(
                event=event["event"],
                question_uuid=self.question_uuid,
                parent_question_uuid=None,
                originator_question_uuid=self.question_uuid,
                attributes=event["attributes"],
                parent=self.parent.id,
                originator=self.parent.id,
                recipient=self.parent.id,
                retry_count=0,
            )

        with patch(
            "octue.cloud.pub_sub.events.GoogleCloudPubSubEventHandler._time_since_last_heartbeat",
            datetime.timedelta(seconds=0),
        ):
            event_handler.handle_events(maximum_heartbeat_interval=0)

    def test_time_since_last_heartbeat_is_none_if_no_heartbeat_received_yet(self):
        """Test that the time since the last heartbeat is `None` if no heartbeat has been received yet."""
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)
        self.assertIsNone(event_handler._time_since_last_heartbeat)

    def test_total_run_time_is_none_if_handle_events_has_not_been_called(self):
        """Test that the total run time for the event handler is `None` if the `handle_events` method has not been
        called.
        """
        event_handler = GoogleCloudPubSubEventHandler(subscription=self.subscription)
        self.assertIsNone(event_handler.total_run_time)


class TestPullAvailableEvents(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        cls.service_patcher.start()
        cls.question_uuid = str(uuid.uuid4())

        cls.subscription = MockSubscription(
            name=f"my-org.my-service.1-0-0.answers.{cls.question_uuid}",
            topic=cls.test_result_modifier.services_topic,
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

    def test_pull_available_events(self):
        """Test that pulling and enqueuing an event works."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            event_handlers={
                "test": lambda event, attributes: None,
                "finish-test": lambda event, attributes: "This is the result.",
            },
            schema={},
        )

        # Enqueue a mock event for a mock subscription to receive.
        mock_event = {"kind": "test"}

        MESSAGES[self.question_uuid] = [
            MockMessage.from_primitive(
                mock_event,
                attributes={
                    "question_uuid": self.question_uuid,
                    "parent": self.parent.id,
                    "sender": self.parent.id,
                    "sender_type": "CHILD",
                    "sender_sdk_version": "0.50.0",
                    "recipient": "my-org/my-service:1.0.0",
                },
            )
        ]

        events = event_handler._pull_available_events(timeout=10)
        self.assertEqual(events[0][0], mock_event)

    def test_timeout_error_raised_if_result_event_not_received_in_time(self):
        """Test that a timeout error is raised if a result event is not received in time."""
        event_handler = GoogleCloudPubSubEventHandler(
            subscription=self.subscription,
            event_handlers={
                "test": lambda event, attributes: None,
                "finish-test": lambda event, attributes: "This is the result.",
            },
        )

        event_handler._start_time = 0

        with self.assertRaises(TimeoutError):
            event_handler._pull_available_events(timeout=1e-6)
