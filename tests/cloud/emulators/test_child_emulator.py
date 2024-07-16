import json
import os

from octue.cloud.emulators._pub_sub import MockService
from octue.cloud.emulators.child import ChildEmulator
from octue.cloud.emulators.service import ServicePatcher
from octue.resources.service_backends import GCPPubSubBackend
from tests import TESTS_DIR
from tests.base import BaseTestCase


with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
    EVENTS = json.load(f)


ATTRIBUTES = {
    "datetime": "2024-04-11T10:46:48.236064",
    "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
    "retry_count": 0,
    "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
    "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
    "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
    "parent": "octue/test-service:1.0.0",
    "originator": "octue/test-service:1.0.0",
    "sender": "octue/test-service:1.0.0",
    "sender_type": "CHILD",
    "sender_sdk_version": "0.51.0",
    "recipient": "octue/another-service:3.2.1",
}


class TestChildEmulatorAsk(BaseTestCase):

    def test_representation(self):
        """Test that child emulators are represented correctly."""
        self.assertEqual(repr(ChildEmulator(events=EVENTS)), "<ChildEmulator('octue/test-service:1.0.0')>")

    def test_error_raised_if_no_events(self):
        """Test that an error is raised if no events are provided."""
        with self.assertRaises(ValueError):
            ChildEmulator(events=[])

    def test_ask(self):
        """Test that result events are returned by the emulator's ask method."""
        child_emulator = ChildEmulator(events=EVENTS)
        result, question_uuid = child_emulator.ask(input_values={"hello": "world"})

        self.assertEqual(result["output_values"], [1, 2, 3, 4, 5])
        self.assertEqual(result["output_manifest"].id, "a13713ae-f207-41c6-9e29-0a848ced6039")
        self.assertEqual(question_uuid, "d45c7e99-d610-413b-8130-dd6eef46dda6")

    def test_empty_output_returned_by_ask_if_no_result_present_in_events(self):
        """Test that an empty output is returned if no result event is present in the given events."""
        child_emulator = ChildEmulator(events=EVENTS[:-2])
        result, question_uuid = child_emulator.ask(input_values={"hello": "world"})

        self.assertIsNone(result)
        self.assertEqual(question_uuid, "d45c7e99-d610-413b-8130-dd6eef46dda6")

    def test_ask_with_exception(self):
        """Test that exceptions are raised by the emulator."""
        events = [
            *EVENTS[:-2],
            {
                "event": {
                    "kind": "exception",
                    "exception_type": "TypeError",
                    "exception_message": "This simulates an error in the child.",
                    "exception_traceback": [""],
                },
                "attributes": ATTRIBUTES,
            },
        ]

        child_emulator = ChildEmulator(events=events)

        # Test that the exception was raised.
        with self.assertRaises(TypeError) as context:
            child_emulator.ask(input_values={"hello": "world"})

        # Test that the exception was raised in the parent and not the child.
        self.assertIn(
            "The following traceback was captured from the remote service 'octue/test-service:1.0.0':",
            format(context.exception),
        )

    def test_ask_with_monitor_message(self):
        """Test that monitor messages are handled by the emulator."""
        events = [
            {
                "event": {
                    "kind": "monitor_message",
                    "data": "A sample monitor message.",
                },
                "attributes": ATTRIBUTES,
            },
        ]

        child_emulator = ChildEmulator(events=events)

        monitor_messages = []

        child_emulator.ask(
            input_values={"hello": "world"},
            handle_monitor_message=lambda value: monitor_messages.append(value),
        )

        # Check that the monitor message handler has worked.
        self.assertEqual(monitor_messages, ["A sample monitor message."])

    def test_events_recorded_from_real_child_can_be_used_in_child_emulator(self):
        """Test that events recorded from a real child can be used as emulated events in a child emulator (i.e. test
        that the event format is unified between `Service` and `ChildEmulator`).
        """
        backend = GCPPubSubBackend(project_name="my-project")

        def error_run_function(*args, **kwargs):
            raise OSError("Oh no. Some error has been raised for testing.")

        child = MockService(backend=backend, run_function=error_run_function)
        parent = MockService(backend=backend, children={child.id: child})

        with ServicePatcher():
            child.serve()

            with self.assertRaises(OSError):
                subscription, _ = parent.ask(service_id=child.id, input_values={})
                parent.wait_for_answer(subscription=subscription)

        child_emulator = ChildEmulator(events=parent.received_events)

        with self.assertRaises(OSError):
            child_emulator.ask(input_values={})

    def test_ask_more_than_one_question_in_serial(self):
        """Test that a child emulator can be asked more than one question in serial without an error occurring."""
        child_emulator = ChildEmulator(events=EVENTS)
        result_0, _ = child_emulator.ask(input_values={"hello": "world"})
        result_1, _ = child_emulator.ask(input_values={"hello": "planet"})
        self.assertEqual(result_0["output_values"], result_1["output_values"])
