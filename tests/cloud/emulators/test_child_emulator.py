import logging
import os

from octue.cloud import storage
from octue.cloud.emulators._pub_sub import MockService, MockTopic
from octue.cloud.emulators.child import ChildEmulator, ServicePatcher
from octue.cloud.events import OCTUE_SERVICES_PREFIX
from octue.cloud.storage import GoogleCloudStorageClient
from octue.resources import Manifest
from octue.resources.service_backends import GCPPubSubBackend
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME, TEST_PROJECT_NAME, TESTS_DIR
from tests.base import BaseTestCase


class TestChildEmulatorAsk(BaseTestCase):

    BACKEND = {"name": "GCPPubSubBackend", "project_name": "blah"}

    @classmethod
    def setUpClass(cls):
        topic = MockTopic(name=OCTUE_SERVICES_PREFIX, project_name=TEST_PROJECT_NAME)

        with ServicePatcher():
            topic.create(allow_existing=True)

    def test_representation(self):
        """Test that child emulators are represented correctly."""
        self.assertEqual(
            repr(ChildEmulator(id=f"octue/emulated-child:{MOCK_SERVICE_REVISION_TAG}")),
            f"<ChildEmulator('octue/emulated-child:{MOCK_SERVICE_REVISION_TAG}')>",
        )

    def test_ask_with_non_dictionary_message(self):
        """Test that messages that aren't dictionaries fail validation."""
        messages = [
            ["hello"],
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_message_structure(self):
        """Test that messages with an invalid structure fail validation."""
        messages = [
            {"message": "hello"},
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_message_type(self):
        """Test that messages with an invalid type fail validation."""
        messages = [
            {"kind": "hello", "content": [1, 2, 3, 4]},
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    # Re-enable this when schema validation has been sorted out in the child emulator.
    # def test_ask_with_invalid_result(self):
    #     """Test that an invalid result fails validation."""
    #     messages = [
    #         {
    #             "kind": "result",
    #             "wrong": "keys",
    #         },
    #     ]
    #
    #     child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)
    #
    #     with self.assertRaises(ValueError):
    #         child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_result_message(self):
        """Test that result messages are returned by the emulator's ask method."""
        output_manifest = Manifest()

        messages = [
            {
                "kind": "result",
                "output_values": [1, 2, 3, 4],
                "output_manifest": output_manifest,
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result["output_values"], [1, 2, 3, 4])
        self.assertEqual(result["output_manifest"].id, output_manifest.id)

    def test_ask_with_input_manifest(self):
        """Test that a child emulator can accept an input manifest."""
        input_manifest = Manifest()

        messages = [
            {
                "kind": "result",
                "output_values": [1, 2, 3, 4],
                "output_manifest": None,
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        result = child_emulator.ask(input_values={"hello": "world"}, input_manifest=input_manifest)
        self.assertEqual(result["output_values"], [1, 2, 3, 4])

    def test_empty_output_returned_by_ask_if_no_result_present_in_messages(self):
        """Test that an empty output is returned if no result message is present in the given messages."""
        child_emulator = ChildEmulator(backend=self.BACKEND, messages=[])
        result = child_emulator.ask(input_values={"hello": "world"})
        self.assertEqual(result, {"output_values": None, "output_manifest": None})

    def test_ask_with_log_record_with_missing_log_record_key(self):
        """Test that an error is raised if a log record message missing the "log_record" key is given."""
        messages = [
            {
                "kind": "log_record",
            }
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_invalid_log_record(self):
        """Test that an invalid log record representation fails validation."""
        messages = [
            {
                "kind": "log_record",
                "log_record": [1, 2, 3],
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(TypeError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_logs(self):
        """Test that log records can be handled by the emulator."""
        messages = [
            {
                "kind": "log_record",
                "log_record": {"msg": "Starting analysis.", "levelno": 20, "levelname": "INFO"},
            },
            {
                "kind": "log_record",
                "log_record": {"msg": "Finishing analysis.", "levelno": 20, "levelname": "INFO"},
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertLogs(level=logging.INFO) as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertEqual(logging_context.records[6].message, "Starting analysis.")
        self.assertEqual(logging_context.records[7].message, "Finishing analysis.")

    def test_ask_with_logs_without_level_number_and_name(self):
        """Test that the 'INFO' log level is used if none is provided in the log record dictionaries."""
        messages = [
            {
                "kind": "log_record",
                "log_record": {"msg": "Starting analysis."},
            },
            {
                "kind": "log_record",
                "log_record": {"msg": "Finishing analysis."},
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertLogs(level=logging.INFO) as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertEqual(logging_context.records[6].levelname, "INFO")
        self.assertEqual(logging_context.records[6].message, "Starting analysis.")

        self.assertEqual(logging_context.records[7].levelname, "INFO")
        self.assertEqual(logging_context.records[7].message, "Finishing analysis.")

    def test_ask_with_invalid_exception(self):
        """Test that an invalid exception fails validation."""
        messages = [
            {
                "kind": "exception",
                "not": "an exception",
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertRaises(ValueError):
            child_emulator.ask(input_values={"hello": "world"})

    def test_ask_with_exception(self):
        """Test that exceptions are raised by the emulator."""
        messages = [
            {
                "kind": "exception",
                "exception_type": "TypeError",
                "exception_message": "This simulates an error in the child.",
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        # Test that the exception was raised.
        with self.assertRaises(TypeError) as context:
            child_emulator.ask(input_values={"hello": "world"})

        # Test that the exception was raised in the parent and not the child.
        self.assertIn("The following traceback was captured from the remote service", format(context.exception))

    def test_ask_with_monitor_message(self):
        """Test that monitor messages are handled by the emulator."""
        messages = [
            {
                "kind": "monitor_message",
                "data": "A sample monitor message.",
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        monitor_messages = []

        child_emulator.ask(
            input_values={"hello": "world"},
            handle_monitor_message=lambda value: monitor_messages.append(value),
        )

        # Check that the monitor message handler has worked.
        self.assertEqual(monitor_messages, ["A sample monitor message."])

    def test_heartbeat_messages_are_ignored(self):
        """Test that heartbeat messages are ignored by the emulator."""
        messages = [
            {
                "kind": "heartbeat",
                "datetime": "2023-11-23T14:25:38.142884",
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)

        with self.assertLogs(level=logging.WARNING) as logging_context:
            child_emulator.ask(input_values={"hello": "world"})

        self.assertIn("Heartbeat messages are ignored by the ChildEmulator.", logging_context.output[0])

    def test_messages_recorded_from_real_child_can_be_used_in_child_emulator(self):
        """Test that messages recorded from a real child can be used as emulated messages in a child emulator (i.e. test
        that the message format is unified between `Service` and `ChildEmulator`).
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

        child_emulator = ChildEmulator(messages=parent.received_events)

        with self.assertRaises(OSError):
            child_emulator.ask(input_values={})

    def test_ask_more_than_one_question(self):
        """Test than a child emulator can be asked more than one question without an error occurring."""
        messages = [
            {
                "kind": "result",
                "output_values": [1, 2, 3, 4],
                "output_manifest": None,
            },
        ]

        child_emulator = ChildEmulator(backend=self.BACKEND, messages=messages)
        result_0 = child_emulator.ask(input_values={"hello": "world"})
        result_1 = child_emulator.ask(input_values={"hello": "planet"})
        self.assertEqual(result_0, result_1)


class TestChildEmulatorJSONFiles(BaseTestCase):

    TEST_FILES_DIRECTORY = os.path.join(TESTS_DIR, "cloud", "emulators", "valid_child_emulator_files")

    @classmethod
    def setUpClass(cls):
        topic = MockTopic(name=OCTUE_SERVICES_PREFIX, project_name=TEST_PROJECT_NAME)

        with ServicePatcher():
            topic.create(allow_existing=True)

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

        with self.assertLogs(level=logging.INFO) as logging_context:
            result = child_emulator.ask(
                input_values={"hello": "world"},
                handle_monitor_message=lambda value: monitor_messages.append(value),
            )

        # Check log records have been emitted.
        self.assertEqual(logging_context.records[6].message, "Starting analysis.")
        self.assertEqual(logging_context.records[7].message, "Finishing analysis.")

        # Check monitor message has been handled.
        self.assertEqual(monitor_messages, [{"sample": "data"}])

        # Check result has been produced and received.
        self.assertEqual(result, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})

    def test_with_full_file(self):
        """Test that a child emulator can be instantiated from a JSON file containing all its instantiation arguments,
        asked a question, and produce the correct messages.
        """
        child_emulator = ChildEmulator.from_file(os.path.join(self.TEST_FILES_DIRECTORY, "full_file.json"))
        self.assertEqual(child_emulator.id, f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}")
        self.assertEqual(child_emulator._child.backend.project_name, "blah")
        self.assertEqual(child_emulator._parent.id, f"octue/my-service:{MOCK_SERVICE_REVISION_TAG}")

        monitor_messages = []

        with self.assertLogs(level=logging.INFO) as logging_context:
            result = child_emulator.ask(
                input_values={"hello": "world"},
                handle_monitor_message=lambda value: monitor_messages.append(value),
            )

        # Check log records have been emitted.
        self.assertEqual(logging_context.records[6].message, "Starting analysis.")
        self.assertEqual(logging_context.records[7].message, "Finishing analysis.")

        # Check monitor message has been handled.
        self.assertEqual(monitor_messages, [{"sample": "data"}])

        # Check result has been produced and received.
        self.assertEqual(result, {"output_values": [1, 2, 3, 4, 5], "output_manifest": None})

    def test_with_exception(self):
        """Test that a child emulator can be instantiated from a JSON file including an exception in its messages and,
        when asked a question, produce the messages prior to the exception before raising the exception.
        """
        child_emulator = ChildEmulator.from_file(os.path.join(self.TEST_FILES_DIRECTORY, "file_with_exception.json"))

        with self.assertLogs(level=logging.INFO) as logging_context:
            with self.assertRaises(FileNotFoundError):
                child_emulator.ask(input_values={"hello": "world"})

        # Check log records were emitted before the error was raised.
        self.assertIn(logging_context.records[6].message, "Starting analysis.")

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
