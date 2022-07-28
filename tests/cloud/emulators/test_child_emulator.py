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
