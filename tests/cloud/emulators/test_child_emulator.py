import logging

from octue.cloud.emulators.child import ChildEmulator
from tests.base import BaseTestCase


class TestChildEmulator(BaseTestCase):
    def test_with_result_message(self):
        messages = [{"type": "result", "content": {"output_values": [1, 2, 3, 4], "output_manifest": None}}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": [1, 2, 3, 4], "output_manifest": None})

    def test_empty_output_returned_if_no_result_included_in_messages(self):
        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=[],
        )

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_with_logs_and_result_message(self):
        messages = [
            {
                "type": "log_record",
                "content": logging.makeLogRecord({"msg": "Starting analysis.", "levelno": 20, "levelname": "INFO"}),
            },
            {
                "type": "log_record",
                "content": logging.makeLogRecord({"msg": "Finishing analysis.", "levelno": 20, "levelname": "INFO"}),
            },
            {"type": "result", "content": {"output_values": [1, 2, 3, 4], "output_manifest": None}},
        ]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        with self.assertLogs() as logging_context:
            result = child_emulator.ask(input_values={"hello": "world"})

        self.assertEqual(result, {"output_values": [1, 2, 3, 4], "output_manifest": None})

        self.assertIn("Starting analysis.", logging_context.output[4])
        self.assertIn("Finishing analysis.", logging_context.output[5])

    def test_with_exception(self):
        messages = [{"type": "exception", "content": TypeError("This simulates an error in the child.")}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_with_monitor_message(self):
        messages = [{"type": "monitor_message", "content": "A sample monitor message."}]

        child_emulator = ChildEmulator(
            id="emulated-child",
            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            messages=messages,
        )

        monitor_messages = []

        child_emulator.ask(
            input_values={"hello": "world"}, handle_monitor_message=lambda value: monitor_messages.append(value)
        )

        # Check that the monitor message handler has worked.
        self.assertEqual(monitor_messages, ["A sample monitor message."])
