from octue.cloud.emulators.child import ChildEmulator
from octue.resources import Manifest
from tests.base import BaseTestCase


class TestChildEmulator(BaseTestCase):

    BACKEND = {"name": "GCPPubSubBackend", "project_name": "blah"}

    def test_ask_with_invalid_messages(self):
        """Test that invalid messages fail validation."""
        messages = [{"message": "hello"}]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_result(self):
        """Test that an invalid result fails validation."""
        messages = [
            {
                "type": "result",
                "content": {"wrong": "keys"},
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_result_message(self):
        """Test that result messages are returned by the emulator's ask method."""
        output_manifest = Manifest()

        messages = [
            {
                "type": "result",
                "content": {"output_values": [1, 2, 3, 4], "output_manifest": output_manifest},
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result["output_values"], [1, 2, 3, 4])
        self.assertEqual(result["output_manifest"].id, output_manifest.id)

    def test_empty_output_returned_by_ask_if_no_result_present_in_messages(self):
        """Test that an empty output is returned if no result message is present in the given messages."""
        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=[])
        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_ask_with_invalid_log_record(self):
        """Test that an invalid log record representation fails validation."""
        messages = [
            {
                "type": "log_record",
                "content": [1, 2, 3],
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_logs(self):
        """Test that log records can be handled by the emulator."""
        messages = [
            {
                "type": "log_record",
                "content": {"msg": "Starting analysis.", "levelno": 20, "levelname": "INFO"},
            },
            {
                "type": "log_record",
                "content": {"msg": "Finishing analysis.", "levelno": 20, "levelname": "INFO"},
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertLogs() as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertIn("Starting analysis.", logging_context.output[4])
        self.assertIn("Finishing analysis.", logging_context.output[5])

    def test_ask_with_logs_without_level_number_and_name(self):
        """Test that the 'INFO' log level is used if none is provided in the log record dictionaries."""
        messages = [
            {
                "type": "log_record",
                "content": {"msg": "Starting analysis."},
            },
            {
                "type": "log_record",
                "content": {"msg": "Finishing analysis."},
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertLogs() as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertEqual(logging_context.records[4].levelname, "INFO")
        self.assertIn("Starting analysis.", logging_context.output[4])

        self.assertEqual(logging_context.records[5].levelname, "INFO")
        self.assertIn("Finishing analysis.", logging_context.output[5])

    def test_ask_with_invalid_exception(self):
        """Test that an invalid exception fails validation."""
        messages = [
            {
                "type": "exception",
                "content": "not an exception",
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_exception(self):
        """Test that exceptions are raised by the emulator."""
        messages = [
            {
                "type": "exception",
                "content": ValueError("This simulates an error in the child."),
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        # Test that the exception was raised.
        with self.assertRaises(ValueError) as context:
            child_emulator.ask(input_values={"hello": "world"})

        # Test that the exception was raised in the parent and not the child.
        self.assertIn(
            "The following traceback was captured from the remote service 'emulated-child'", format(context.exception)
        )

    def test_ask_with_monitor_message(self):
        """Test that monitor messages are handled by the emulator."""
        messages = [
            {
                "type": "monitor_message",
                "content": "A sample monitor message.",
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        monitor_messages = []

        child_emulator.ask(
            input_values={"hello": "world"},
            handle_monitor_message=lambda value: monitor_messages.append(value),
        )

        # Check that the monitor message handler has worked.
        self.assertEqual(monitor_messages, ["A sample monitor message."])
