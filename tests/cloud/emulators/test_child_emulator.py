import json
import os

from octue.cloud import storage
from octue.cloud.emulators.child import ChildEmulator
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Manifest
from tests import TEST_BUCKET_NAME, TESTS_DIR
from tests.base import BaseTestCase


class TestChildEmulatorAsk(BaseTestCase):

    BACKEND = {"name": "GCPPubSubBackend", "project_name": "blah"}

    def test_ask_with_invalid_message_structure(self):
        """Test that messages with an invalid structure fail validation."""
        messages = [
            {"message": "hello"},
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_message_type(self):
        """Test that messages with an invalid type fail validation."""
        messages = [
            {"type": "hello", "content": [1, 2, 3, 4]},
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_result(self):
        """Test that an invalid result fails validation."""
        messages = [
            {
                "type": "result",
                "wrong": "keys",
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
                "output_values": [1, 2, 3, 4],
                "output_manifest": output_manifest,
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
                "log_record": [1, 2, 3],
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
                "log_record": {"msg": "Starting analysis.", "levelno": 20, "levelname": "INFO"},
            },
            {
                "type": "log_record",
                "log_record": {"msg": "Finishing analysis.", "levelno": 20, "levelname": "INFO"},
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
                "log_record": {"msg": "Starting analysis."},
            },
            {
                "type": "log_record",
                "log_record": {"msg": "Finishing analysis."},
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
                "not": "an exception",
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_exception(self):
        """Test that exceptions are raised by the emulator."""
        messages = [
            {
                "type": "exception",
                "exception_type": "TypeError",
                "exception_message": "This simulates an error in the child.",
            },
        ]

        child_emulator = ChildEmulator(id="emulated-child", backend=self.BACKEND, messages=messages)

        # Test that the exception was raised.
        with self.assertRaises(TypeError) as context:
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
                "data": json.dumps("A sample monitor message."),
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


class TestChildEmulatorJSONFiles(BaseTestCase):

    TEST_FILES_DIRECTORY = os.path.join(TESTS_DIR, "cloud", "emulators", "valid_child_emulator_files")

    def test_with_empty_file(self):
        """Test that a child emulator can be instantiated from an empty JSON file (a JSON file with only an empty
        object in), asked a question, and produce a trivial result.
        """
        child_emulator = ChildEmulator.from_file(os.path.join(self.TEST_FILES_DIRECTORY, "empty_file.json"))
        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_with_only_messages(self):
        """Test that a child emulator can be instantiated from a JSON file containing only the messages it should
        produce, asked a question, and produce the expected log messages, monitor messages, and result.
        """
        emulator_file_path = os.path.join(self.TEST_FILES_DIRECTORY, "file_with_only_messages.json")
        child_emulator = ChildEmulator.from_file(emulator_file_path)
        self.assertEqual(child_emulator._child.backend.project_name, "emulated-project")

        monitor_messages = []

        with self.assertLogs() as logging_context:
            result = child_emulator.ask(
                input_values={"hello": "world"},
                handle_monitor_message=lambda value: monitor_messages.append(value),
            )

        # Check log records have been emitted.
        self.assertIn("Starting analysis.", logging_context.output[4])
        self.assertIn("Finishing analysis.", logging_context.output[5])

        # Check monitor message has been handled.
        self.assertEqual(monitor_messages, [{"sample": "data"}])

        # Check result has been produced and received.
        self.assertEqual(result, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})

    def test_with_full_file(self):
        """Test that a child emulator can be instantiated from a JSON file containing all its instantiation arguments,
        asked a question, and produce the correct messages.
        """
        child_emulator = ChildEmulator.from_file(os.path.join(self.TEST_FILES_DIRECTORY, "full_file.json"))
        self.assertEqual(child_emulator.id, "octue/my-child")
        self.assertEqual(child_emulator._child.backend.project_name, "blah")
        self.assertEqual(child_emulator._parent.name, "octue.services.my-service")

        monitor_messages = []

        with self.assertLogs() as logging_context:
            result = child_emulator.ask(
                input_values={"hello": "world"},
                handle_monitor_message=lambda value: monitor_messages.append(value),
            )

        # Check log records have been emitted.
        self.assertIn("Starting analysis.", logging_context.output[4])
        self.assertIn("Finishing analysis.", logging_context.output[5])

        # Check monitor message has been handled.
        self.assertEqual(monitor_messages, [{"sample": "data"}])

        # Check result has been produced and received.
        self.assertEqual(result, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})

    def test_with_exception(self):
        """Test that a child emulator can be instantiated from a JSON file including an exception in its messages and,
        when asked a question, produce the messages prior to the exception before raising the exception.
        """
        child_emulator = ChildEmulator.from_file(os.path.join(self.TEST_FILES_DIRECTORY, "file_with_exception.json"))

        with self.assertLogs() as logging_context:
            with self.assertRaises(FileNotFoundError):
                child_emulator.ask(input_values={"hello": "world"})

        # Check log records were emitted before the error was raised.
        self.assertIn("Starting analysis.", logging_context.output[4])

    def test_with_output_manifest(self):
        """Test that a child emulator will return the expected output manifest when given a serialised one in a JSON
        file.
        """
        emulator_file_path = os.path.join(self.TEST_FILES_DIRECTORY, "file_with_output_manifest.json")
        child_emulator = ChildEmulator.from_file(emulator_file_path)

        # Create the datasets in cloud storage.
        storage_client = GoogleCloudStorageClient()

        storage_client.upload_from_string(
            "[1, 2, 3]",
            storage.path.generate_gs_path(TEST_BUCKET_NAME, "dataset_a", "file_0.txt"),
        )

        storage_client.upload_from_string(
            "[4, 5, 6]",
            storage.path.generate_gs_path(TEST_BUCKET_NAME, "dataset_b", "the_data.txt"),
        )

        expected_output_manifest = Manifest(
            id="04a969e9-04b4-4f35-bd30-36de6e7daea2",
            datasets={
                "dataset_a": f"gs://{TEST_BUCKET_NAME}/dataset_a",
                "dataset_b": f"gs://{TEST_BUCKET_NAME}/dataset_b",
            },
        )

        result = child_emulator.ask(input_values={"hello": "world"})

        # Check that the output manifest has been produced correctly.
        self.assertEqual(result["output_manifest"].id, expected_output_manifest.id)
        self.assertEqual(result["output_manifest"].datasets.keys(), expected_output_manifest.datasets.keys())

        self.assertEqual(
            result["output_manifest"].datasets["dataset_a"].files.one().cloud_path,
            expected_output_manifest.datasets["dataset_a"].files.one().cloud_path,
        )

        self.assertEqual(
            result["output_manifest"].datasets["dataset_b"].files.one().cloud_path,
            expected_output_manifest.datasets["dataset_b"].files.one().cloud_path,
        )
