import logging

from octue.cloud.emulators.child import ChildEmulator
from octue.resources import Manifest
from tests.base import BaseTestCase


class TestChildEmulator(BaseTestCase):
    def test_ask_with_result_message(self):
        """Test that result messages are returned by the emulator's ask method."""
        output_manifest = Manifest()
        messages = [{"type": "result", "content": {"output_values": [1, 2, 3, 4], "output_manifest": output_manifest}}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result["output_values"], [1, 2, 3, 4])
        self.assertEqual(result["output_manifest"].id, output_manifest.id)

    def test_empty_output_returned_by_ask_if_no_result_present_in_messages(self):
        """Test that an empty output is returned if no result message is present in the given messages."""
        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=[],
        )

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_ask_with_logs(self):
        """Test that log records can be handled by the emulator."""
        messages = [
            {
                "type": "log_record",
                "content": logging.makeLogRecord({"msg": "Starting analysis.", "levelno": 20, "levelname": "INFO"}),
            },
            {
                "type": "log_record",
                "content": logging.makeLogRecord({"msg": "Finishing analysis.", "levelno": 20, "levelname": "INFO"}),
            },
        ]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        with self.assertLogs() as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertIn("Starting analysis.", logging_context.output[4])
        self.assertIn("Finishing analysis.", logging_context.output[5])

    def test_ask_with_exception(self):
        """Test that exceptions are raised by the emulator."""
        messages = [{"type": "exception", "content": TypeError("This simulates an error in the child.")}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_monitor_message(self):
        """Test that monitor messages are handled by the emulator."""
        messages = [{"type": "monitor_message", "content": "A sample monitor message."}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        monitor_messages = []

        child_emulator.ask(
            input_values={"hello": "world"},
            handle_monitor_message=lambda value: monitor_messages.append(value),
        )

        # Check that the monitor message handler has worked.
        self.assertEqual(monitor_messages, ["A sample monitor message."])
