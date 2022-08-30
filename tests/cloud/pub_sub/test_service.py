import datetime
import functools
import json
import logging
import tempfile
import time
import uuid
from unittest.mock import patch

import twined.exceptions
from octue import Runner, exceptions
from octue.cloud.emulators._pub_sub import (
    DifferentMockAnalysis,
    MockAnalysis,
    MockAnalysisWithOutputManifest,
    MockPullResponse,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)
from octue.cloud.emulators.child import ServicePatcher
from octue.cloud.emulators.cloud_storage import mock_generate_signed_url
from octue.cloud.pub_sub.service import Service
from octue.exceptions import InvalidMonitorMessage
from octue.resources import Datafile, Dataset, Manifest
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


logger = logging.getLogger(__name__)


BACKEND = GCPPubSubBackend(project_name=TEST_PROJECT_NAME)


class TestService(BaseTestCase):
    """Some of these tests require a connection to either a real Google Pub/Sub instance on Google Cloud Platform
    (GCP), or a local emulator.
    """

    service_patcher = ServicePatcher()

    def test_namespace_always_appears_in_id(self):
        """Test that the Octue service namespace always appears at the start of a service's ID whether it's explicitly
        provided or not.
        """
        service_with_no_namespace_in_id = Service(backend=BACKEND, service_id="hello")
        self.assertEqual(service_with_no_namespace_in_id.id, "octue.services.hello")

        service_with_namespace_in_id = Service(backend=BACKEND, service_id="octue.services.hello")
        self.assertEqual(service_with_namespace_in_id.id, "octue.services.hello")

    def test_repr(self):
        """Test that services are represented as a string correctly."""
        service = Service(backend=BACKEND)
        self.assertEqual(repr(service), f"<Service({service.name!r})>")

    def test_service_id_cannot_be_non_none_empty_value(self):
        """Ensure that a ValueError is raised if a non-None empty value is provided as the service_id."""
        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id="")

        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id=[])

        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id={})

    def test_ask_on_non_existent_service_results_in_error(self):
        """Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error.
        """
        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with self.assertRaises(exceptions.ServiceNotFound):
                MockService(backend=BACKEND).ask(service_id="hello", input_values=[1, 2, 3, 4])

    def test_timeout_error_raised_if_no_messages_received_when_waiting(self):
        """Test that a TimeoutError is raised if no messages are received while waiting."""
        service = Service(backend=BACKEND)
        mock_topic = MockTopic(name="world", namespace="hello", service=service)

        mock_subscription = MockSubscription(
            name="world",
            topic=mock_topic,
            namespace="hello",
            project_name=TEST_PROJECT_NAME,
            subscriber=MockSubscriber(),
        )

        with patch("octue.cloud.pub_sub.service.pubsub_v1.SubscriberClient.pull", return_value=MockPullResponse()):
            with self.assertRaises(TimeoutError):
                service.wait_for_answer(subscription=mock_subscription, timeout=0.01)

    def test_error_raised_if_attempting_to_wait_for_answer_from_push_subscription(self):
        """Test that an error is raised if attempting to wait for an answer from a push subscription."""
        service = Service(backend=BACKEND)

        mock_subscription = MockSubscription(
            name="world",
            topic=MockTopic(name="world", namespace="hello", service=service),
            namespace="hello",
            project_name=TEST_PROJECT_NAME,
            subscriber=MockSubscriber(),
            push_endpoint="https://example.com/endpoint",
        )

        with self.assertRaises(exceptions.PushSubscriptionCannotBePulled):
            service.wait_for_answer(subscription=mock_subscription)

    def test_exceptions_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions raised in the child service are handled and sent back to the asker."""
        child = self.make_new_child_with_error(
            twined.exceptions.InvalidManifestContents("'met_mast_id' is a required property")
        )

        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with self.assertRaises(twined.exceptions.InvalidManifestContents) as context:
                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                )

        self.assertIn("'met_mast_id' is a required property", context.exception.args[0])

    def test_exceptions_with_multiple_arguments_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions with multiple arguments raised in the child service are handled and sent back to
        the asker.
        """
        child = self.make_new_child_with_error(FileNotFoundError(2, "No such file or directory: 'blah'"))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with self.assertRaises(FileNotFoundError) as context:
                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                )

        self.assertIn("[Errno 2] No such file or directory: 'blah'", format(context.exception))

    def test_unknown_exceptions_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions not in the exceptions mapping are simply raised as `Exception`s by the asker."""

        class AnUnknownException(Exception):
            pass

        child = self.make_new_child_with_error(AnUnknownException("This is an exception unknown to the asker."))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with self.assertRaises(Exception) as context:
                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                )

        self.assertEqual(type(context.exception).__name__, "AnUnknownException")
        self.assertIn("This is an exception unknown to the asker.", context.exception.args[0])

    def test_ask_with_real_run_function_with_no_log_message_forwarding(self):
        """Test that a service can ask a question to another service that is serving and receive an answer. Use a real
        run function rather than a mock so that the underlying `Runner` instance is used, and check that remote log
        messages aren't forwarded to the local logger.
        """
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.assertLogs() as logging_context:
            with self.service_patcher:
                child.serve()

                answer = self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    subscribe_to_logs=False,
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
        )

        self.assertTrue(all("[REMOTE]" not in message for message in logging_context.output))

    def test_ask_with_real_run_function_with_log_message_forwarding(self):
        """Test that a service can ask a question to another service that is serving and receive an answer. Use a real
        run function rather than a mock so that the underlying `Runner` instance is used, and check that remote log
        messages are forwarded to the local logger.
        """
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.assertLogs() as logs_context_manager:
            with self.service_patcher:
                child.serve()

                answer = self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    subscribe_to_logs=True,
                    service_name="my-super-service",
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
        )

        # Check that the two expected remote log messages were logged consecutively in the right order with the service
        # name added as context at the start of the messages.
        start_remote_analysis_message_present = False
        finish_remote_analysis_message_present = False

        for i, log_record in enumerate(logs_context_manager.records):
            if "[my-super-service" in log_record.msg and "Starting analysis." in log_record.msg:
                start_remote_analysis_message_present = True

                if (
                    "[my-super-service" in logs_context_manager.records[i + 1].msg
                    and "Finished analysis." in logs_context_manager.records[i + 1].msg
                ):
                    finish_remote_analysis_message_present = True

                break

        self.assertTrue(start_remote_analysis_message_present)
        self.assertTrue(finish_remote_analysis_message_present)

    def test_ask_with_forwarding_exception_log_message(self):
        """Test that exception/error logs are forwarded to the asker successfully."""

        def create_exception_logging_run_function():
            def mock_app(analysis):
                try:
                    raise OSError("This is an OSError.")
                except OSError:
                    logger.exception("An example exception to log and forward to the parent.")

            return Runner(app_src=mock_app, twine='{"input_values_schema": {"type": "object", "required": []}}').run

        child = MockService(backend=BACKEND, run_function=create_exception_logging_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.assertLogs() as logs_context_manager:
            with self.service_patcher:
                child.serve()

                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    subscribe_to_logs=True,
                    service_name="my-super-service",
                )

        error_logged = False

        for record in logs_context_manager.records:
            if (
                record.levelno == logging.ERROR
                and "An example exception to log and forward to the parent." in record.message
                and "This is an OSError" in record.exc_text
            ):
                error_logged = True
                break

        self.assertTrue(error_logged)

    def test_with_monitor_message_handler(self):
        """Test that monitor messages can be sent from a child app and handled by the parent's monitor message handler."""

        def create_run_function_with_monitoring():
            def mock_app(analysis):
                analysis.send_monitor_message({"status": "my first monitor message"})
                analysis.send_monitor_message({"status": "my second monitor message"})

            twine = """
                {
                    "input_values_schema": {"type": "object", "required": []},
                    "monitor_message_schema": {
                        "type": "object",
                        "properties": {"status": {"type": "string"}},
                        "required": ["status"]
                    }
                }
            """

            return Runner(app_src=mock_app, twine=twine).run

        child = MockService(backend=BACKEND, run_function=create_run_function_with_monitoring())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            subscription, _ = parent.ask(child.id, input_values={})
            monitoring_data = []

            parent.wait_for_answer(
                subscription,
                handle_monitor_message=lambda data: monitoring_data.append(data),
            )

        self.assertEqual(
            monitoring_data, [{"status": "my first monitor message"}, {"status": "my second monitor message"}]
        )

    def test_monitoring_update_fails_if_schema_not_met(self):
        """Test that an error is raised and sent to the analysis logger if a monitor message fails schema validation,
        but earlier valid monitor messages still make it to the parent's monitoring callback.
        """

        def create_run_function_with_monitoring():
            def mock_app(analysis):
                analysis.send_monitor_message({"status": "my first monitor message"})
                analysis.send_monitor_message({"wrong": "my second monitor message"})
                analysis.send_monitor_message({"status": "my third monitor message"})

            twine = """
                {
                    "input_values_schema": {"type": "object", "required": []},
                    "monitor_message_schema": {
                        "type": "object",
                        "properties": {"status": {"type": "string"}},
                        "required": ["status"]
                    }
                }
            """

            return Runner(app_src=mock_app, twine=twine).run

        child = MockService(backend=BACKEND, run_function=create_run_function_with_monitoring())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            subscription, _ = parent.ask(child.id, input_values={})
            monitoring_data = []

            with self.assertRaises(InvalidMonitorMessage):
                parent.wait_for_answer(
                    subscription,
                    handle_monitor_message=lambda data: monitoring_data.append(data),
                )

        self.assertEqual(
            monitoring_data,
            [{"status": "my first monitor message"}],
        )

    def test_ask_with_input_manifest(self):
        """Test that a service can ask a question including an input manifest to another service that is serving and
        receive an answer.
        """
        child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysis())
        parent = MockService(backend=BACKEND, children={child.id: child})

        dataset_path = f"gs://{TEST_BUCKET_NAME}/my-dataset"

        input_manifest = Manifest(
            datasets={
                "my-dataset": Dataset(
                    files=[f"{dataset_path}/hello.txt", f"{dataset_path}/goodbye.csv"],
                    path=dataset_path,
                )
            }
        )

        with self.service_patcher:
            child.serve()

            with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
                answer = self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    input_manifest=input_manifest,
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
        )

    def test_ask_with_input_manifest_and_no_input_values(self):
        """Test that a service can ask a question including an input manifest and no input values to another service
        that is serving and receive an answer.
        """
        child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysis())
        parent = MockService(backend=BACKEND, children={child.id: child})

        dataset_path = f"gs://{TEST_BUCKET_NAME}/my-dataset"

        input_manifest = Manifest(
            datasets={
                "my-dataset": Dataset(
                    files=[f"{dataset_path}/hello.txt", f"{dataset_path}/goodbye.csv"],
                    path=dataset_path,
                )
            }
        )

        with self.service_patcher:
            child.serve()

            with patch("google.cloud.storage.blob.Blob.generate_signed_url", new=mock_generate_signed_url):
                answer = self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_manifest=input_manifest,
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
        )

    def test_ask_with_input_manifest_with_local_paths_raises_error(self):
        """Test that an error is raised if an input manifest whose datasets and/or files are not located in the cloud
        is used in a question.
        """
        with self.assertRaises(exceptions.FileLocationError):
            MockService(backend=BACKEND).ask(
                service_id=str(uuid.uuid4()),
                input_values={},
                input_manifest=self.create_valid_manifest(),
            )

    def test_ask_with_input_manifest_with_local_paths_works_if_allowed_and_child_has_access_to_the_local_paths(self):
        """Test that an input manifest referencing local files can be used if the files can be accessed by the child and
        the `allow_local_files` parameter is `True`.
        """
        temporary_local_path = tempfile.NamedTemporaryFile(delete=False).name

        with open(temporary_local_path, "w") as f:
            f.write("This is a local file.")

        local_file = Datafile(path=temporary_local_path)
        self.assertFalse(local_file.exists_in_cloud)

        manifest = Manifest(datasets={"my-local-dataset": Dataset(name="my-local-dataset", files={local_file})})

        # Get the child to open the local file itself and return the contents as output.
        def run_function(*args, **kwargs):
            with open(temporary_local_path) as f:
                return MockAnalysis(output_values=f.read())

        child = MockService(backend=BACKEND, run_function=run_function)
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            answer = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={},
                input_manifest=manifest,
                allow_local_files=True,
            )

        self.assertEqual(answer["output_values"], "This is a local file.")

    def test_ask_with_output_manifest(self):
        """Test that a service can receive an output manifest as part of the answer to a question."""
        child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysisWithOutputManifest())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            answer = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={},
            )

        self.assertEqual(answer["output_values"], MockAnalysisWithOutputManifest.output_values)
        self.assertEqual(answer["output_manifest"].id, MockAnalysisWithOutputManifest.output_manifest.id)

    def test_service_can_ask_multiple_questions_to_child(self):
        """Test that a service can ask multiple questions to the same child and expect replies to them all."""
        child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysis())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            answers = []

            for i in range(5):
                answers.append(
                    self.ask_question_and_wait_for_answer(
                        parent=parent,
                        child=child,
                        input_values={},
                    )
                )

        for answer in answers:
            self.assertEqual(
                answer,
                {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
            )

    def test_service_can_ask_questions_to_multiple_children(self):
        """Test that a service can ask questions to different children and expect replies to them all."""
        child_1 = self.make_new_child(BACKEND, run_function_returnee=MockAnalysis())
        child_2 = self.make_new_child(BACKEND, run_function_returnee=DifferentMockAnalysis())

        parent = MockService(backend=BACKEND, children={child_1.id: child_1, child_2.id: child_2})

        with self.service_patcher:
            child_1.serve()
            child_2.serve()

            answer_1 = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child_1,
                input_values={},
            )

            answer_2 = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child_2,
                input_values={},
            )

        self.assertEqual(
            answer_1,
            {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
        )

        self.assertEqual(
            answer_2,
            {
                "output_values": DifferentMockAnalysis.output_values,
                "output_manifest": DifferentMockAnalysis.output_manifest,
            },
        )

    def test_child_can_ask_its_own_child_questions(self):
        """Test that a child can contact its own child while answering a question from a parent."""
        child_of_child = self.make_new_child(BACKEND, run_function_returnee=DifferentMockAnalysis())

        def child_run_function(analysis_id, input_values, *args, **kwargs):
            subscription, _ = child.ask(service_id=child_of_child.id, input_values=input_values)
            return MockAnalysis(output_values={input_values["question"]: child.wait_for_answer(subscription)})

        child = MockService(
            backend=BACKEND,
            run_function=child_run_function,
            children={child_of_child.id: child_of_child},
        )

        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            child_of_child.serve()

            answer = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={"question": "What does the child of the child say?"},
            )

        self.assertEqual(
            answer,
            {
                "output_values": {
                    "What does the child of the child say?": {
                        "output_values": DifferentMockAnalysis.output_values,
                        "output_manifest": DifferentMockAnalysis.output_manifest,
                    }
                },
                "output_manifest": None,
            },
        )

    def test_child_can_ask_its_own_children_questions(self):
        """Test that a child can contact more than one of its own children while answering a question from a parent."""
        first_child_of_child = self.make_new_child(BACKEND, run_function_returnee=DifferentMockAnalysis())
        second_child_of_child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysis())

        def child_run_function(analysis_id, input_values, *args, **kwargs):
            subscription_1, _ = child.ask(service_id=first_child_of_child.id, input_values=input_values)
            subscription_2, _ = child.ask(service_id=second_child_of_child.id, input_values=input_values)

            return MockAnalysis(
                output_values={
                    "first_child_of_child": child.wait_for_answer(subscription_1),
                    "second_child_of_child": child.wait_for_answer(subscription_2),
                }
            )

        child = MockService(
            backend=BACKEND,
            run_function=child_run_function,
            children={first_child_of_child.id: first_child_of_child, second_child_of_child.id: second_child_of_child},
        )

        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            first_child_of_child.serve()
            second_child_of_child.serve()

            answer = self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={"question": "What does the child of the child say?"},
            )

        self.assertEqual(
            answer,
            {
                "output_values": {
                    "first_child_of_child": {
                        "output_values": DifferentMockAnalysis.output_values,
                        "output_manifest": DifferentMockAnalysis.output_manifest,
                    },
                    "second_child_of_child": {
                        "output_values": MockAnalysis().output_values,
                        "output_manifest": MockAnalysis().output_manifest,
                    },
                },
                "output_manifest": None,
            },
        )

    def test_warning_issued_if_child_and_parent_sdk_versions_incompatible(self):
        """Test that a warning is logged if the parent and child's Octue SDK versions are potentially incompatible."""
        child = self.make_new_child(backend=BACKEND, run_function_returnee=MockAnalysis())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            for parent_sdk_version, child_sdk_version in (
                ("0.1.0", "0.2.0"),
                ("0.2.0", "0.1.0"),
                ("0.1.0", "1.1.0"),
                ("1.1.0", "0.1.0"),
            ):
                with self.subTest(parent_sdk_version=parent_sdk_version, child_sdk_version=child_sdk_version):
                    with patch("pkg_resources.Distribution.version", child_sdk_version):
                        with self.assertLogs() as logging_context:
                            self.ask_question_and_wait_for_answer(
                                parent=parent,
                                child=child,
                                input_values={"question": "What does the child of the child say?"},
                                parent_sdk_version=parent_sdk_version,
                            )

                            self.assertIn(
                                f"The parent's Octue SDK version {parent_sdk_version} may not be compatible "
                                f"with the local Octue SDK version {child_sdk_version}",
                                logging_context.output[3],
                            )

    def test_messages_sent_to_parent_are_not_recorded_by_child_if_crash_diagnostics_not_allowed(self):
        """Test that messages sent to the parent by the child aren't recorded by the child if crash diagnostics aren't
        allowed.
        """
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={},
                subscribe_to_logs=True,
                allow_save_diagnostics_data_on_crash=False,
                service_name="my-super-service",
            )

        # Check that the child's messages haven't been recorded.
        self.assertEqual(child._sent_messages, [])

    def test_messages_sent_to_parent_are_recorded_by_child_if_crash_diagnostics_allowed(self):
        """Test that messages sent to the parent by the child are recorded by the child if crash diagnostics are
        allowed.
        """
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            self.ask_question_and_wait_for_answer(
                parent=parent,
                child=child,
                input_values={},
                subscribe_to_logs=True,
                allow_save_diagnostics_data_on_crash=True,
                service_name="my-super-service",
            )

        # Check that the child's messages have been recorded.
        self.assertEqual(child._sent_messages[0]["type"], "delivery_acknowledgement")
        self.assertEqual(child._sent_messages[1]["type"], "log_record")
        self.assertEqual(child._sent_messages[2]["type"], "log_record")
        self.assertEqual(child._sent_messages[3]["type"], "log_record")

        self.assertEqual(
            child._sent_messages[4],
            {"type": "result", "output_values": "Hello! It worked!", "output_manifest": None, "message_number": 4},
        )

    def test_child_messages_can_be_recorded_by_parent(self):
        """Test that the parent can record messages it receives from its child to a JSON file."""
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    subscribe_to_logs=True,
                    record_messages_to=temporary_file.name,
                    service_name="my-super-service",
                )

                with open(temporary_file.name) as f:
                    recorded_messages = json.load(f)

        # Check that the child's messages have been recorded by the parent.
        self.assertEqual(recorded_messages[0]["type"], "delivery_acknowledgement")
        self.assertEqual(recorded_messages[1]["type"], "log_record")
        self.assertEqual(recorded_messages[2]["type"], "log_record")
        self.assertEqual(recorded_messages[3]["type"], "log_record")

        self.assertEqual(
            recorded_messages[4],
            {"type": "result", "output_values": "Hello! It worked!", "output_manifest": None, "message_number": 4},
        )

    def test_child_exception_message_can_be_recorded_by_parent(self):
        """Test that the parent can record exceptions raised by the child."""
        child = self.make_new_child_with_error(ValueError("Oh no."))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                with self.assertRaises(ValueError):
                    self.ask_question_and_wait_for_answer(
                        parent=parent,
                        child=child,
                        input_values={},
                        subscribe_to_logs=True,
                        record_messages_to=temporary_file.name,
                        service_name="my-super-service",
                    )

                with open(temporary_file.name) as f:
                    recorded_messages = json.load(f)

        # Check that the child's messages have been recorded by the parent.
        self.assertEqual(recorded_messages[0]["type"], "delivery_acknowledgement")
        self.assertEqual(recorded_messages[2]["type"], "exception")
        self.assertIn("Oh no.", recorded_messages[2]["exception_message"])

    def test_heartbeat_messages_are_sent_at_expected_regular_intervals(self):
        """Test that heartbeat messages are sent at the expected regular intervals."""

        def run_function(*args, **kwargs):
            time.sleep(0.3)
            return MockAnalysis()

        child = MockService(backend=BACKEND, run_function=lambda *args, **kwargs: run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with patch(
                "octue.cloud.emulators._pub_sub.MockService.answer",
                functools.partial(child.answer, heartbeat_interval=0.1),
            ):
                self.ask_question_and_wait_for_answer(
                    parent=parent,
                    child=child,
                    input_values={},
                    subscribe_to_logs=True,
                    allow_save_diagnostics_data_on_crash=True,
                    service_name="my-super-service",
                )

        self.assertEqual(child._sent_messages[1]["type"], "heartbeat")
        self.assertEqual(child._sent_messages[2]["type"], "heartbeat")

        first_heartbeat_time = datetime.datetime.fromisoformat(child._sent_messages[1]["time"])
        second_heartbeat_time = datetime.datetime.fromisoformat(child._sent_messages[2]["time"])

        self.assertAlmostEqual(
            second_heartbeat_time - first_heartbeat_time,
            datetime.timedelta(seconds=0.1),
            delta=datetime.timedelta(0.05),
        )

    @staticmethod
    def make_new_child(backend, run_function_returnee):
        """Make and return a new child service that returns the given run function returnee when its run function is
        executed.

        :param octue.resources.service_backends.ServiceBackend backend:
        :param any run_function_returnee:
        :return octue.cloud.emulators._pub_sub.MockService:
        """
        return MockService(backend=backend, run_function=lambda *args, **kwargs: run_function_returnee)

    def make_new_child_with_error(self, exception_to_raise):
        """Make a mock child service that raises the given exception when its run function is executed.

        :param Exception exception_to_raise:
        :return octue.cloud.emulators._pub_sub.MockService:
        """
        child = self.make_new_child(BACKEND, run_function_returnee=None)

        def error_run_function(*args, **kwargs):
            raise exception_to_raise

        child.run_function = error_run_function
        return child

    @staticmethod
    def ask_question_and_wait_for_answer(
        parent,
        child,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        record_messages_to=None,
        allow_save_diagnostics_data_on_crash=True,
        service_name="my-service",
        question_uuid=None,
        push_endpoint=None,
        timeout=30,
        delivery_acknowledgement_timeout=30,
        parent_sdk_version=None,
    ):
        """Get a parent service to ask a question to a child service and wait for the answer.

        :param octue.cloud.emulators._pub_sub.MockService parent:
        :param octue.cloud.emulators._pub_sub.MockService child:
        :param dict|None input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :param bool subscribe_to_logs:
        :param bool allow_local_files:
        :param str|None record_messages_to:
        :param bool allow_save_diagnostics_data_on_crash:
        :param str service_name:
        :param str|None question_uuid:
        :param callable|None push_endpoint:
        :param int|float timeout:
        :param int|float delivery_acknowledgement_timeout:
        :param str|None parent_sdk_version:
        :return dict:
        """
        subscription, _ = parent.ask(
            service_id=child.id,
            input_values=input_values,
            input_manifest=input_manifest,
            subscribe_to_logs=subscribe_to_logs,
            allow_local_files=allow_local_files,
            allow_save_diagnostics_data_on_crash=allow_save_diagnostics_data_on_crash,
            question_uuid=question_uuid,
            push_endpoint=push_endpoint,
            timeout=timeout,
            parent_sdk_version=parent_sdk_version,
        )

        return parent.wait_for_answer(
            subscription=subscription,
            record_messages_to=record_messages_to,
            service_name=service_name,
            delivery_acknowledgement_timeout=delivery_acknowledgement_timeout,
        )

    @staticmethod
    def create_run_function():
        """Create a run function that sends log messages back to the parent and gives a simple output value.

        :return callable: the run function
        """

        def mock_app(analysis):
            logger.info("Starting analysis.")
            analysis.output_values = "Hello! It worked!"
            analysis.output_manifest = None
            logger.info("Finished analysis.")

        twine = """
            {
                "input_values_schema": {
                    "type": "object",
                    "required": []
                },
                "output_values_schema": {}
            }
        """

        return Runner(app_src=mock_app, twine=twine).run
