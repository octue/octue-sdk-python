import datetime
import functools
import logging
import tempfile
import time
from unittest.mock import patch

import google.api_core.exceptions

import twined.exceptions
from octue import Runner, exceptions
from octue.cloud.emulators._pub_sub import (
    DifferentMockAnalysis,
    MockAnalysis,
    MockAnalysisWithOutputManifest,
    MockPullResponse,
    MockService,
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

    def test_repr(self):
        """Test that services are represented as a string correctly."""
        service = Service(backend=BACKEND)
        self.assertEqual(repr(service), f"<Service({service.id!r})>")

    def test_repr_with_name(self):
        """Test that services are represented using their name if they have one."""
        service = Service(backend=BACKEND, name="octue/blah-service:latest")
        self.assertEqual(repr(service), "<Service('octue/blah-service:latest')>")

    def test_service_id_cannot_be_non_none_empty_value(self):
        """Ensure that a ValueError is raised if a non-None empty value is provided as the service_id."""
        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id="")

        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id=[])

        with self.assertRaises(ValueError):
            Service(backend=BACKEND, service_id={})

    def test_serve_fails_if_service_with_same_id_already_exists(self):
        """Test that serving a service fails if a service with the same name already exists."""
        with self.service_patcher:
            with patch(
                "octue.cloud.pub_sub.service.Topic.create",
                side_effect=google.api_core.exceptions.AlreadyExists(""),
            ):
                with self.assertRaises(exceptions.ServiceAlreadyExists):
                    MockService(backend=BACKEND, service_id="my-org/existing-service:latest").serve()

    def test_serve(self):
        """Test that serving works with a unique service ID."""
        with self.service_patcher:
            MockService(backend=BACKEND, service_id="my-org/existing-service:latest").serve()

    def test_ask_on_non_existent_service_results_in_error(self):
        """Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error.
        """
        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with self.assertRaises(exceptions.ServiceNotFound):
                MockService(backend=BACKEND).ask(service_id="my-org/existing-service:latest", input_values=[1, 2, 3, 4])

    def test_timeout_error_raised_if_no_messages_received_when_waiting(self):
        """Test that a TimeoutError is raised if no messages are received while waiting."""
        mock_topic = MockTopic(name="world", project_name=TEST_PROJECT_NAME)
        mock_subscription = MockSubscription(name="world", topic=mock_topic, project_name=TEST_PROJECT_NAME)
        service = Service(backend=BACKEND)

        with patch("octue.cloud.pub_sub.service.pubsub_v1.SubscriberClient.pull", return_value=MockPullResponse()):
            with self.assertRaises(TimeoutError):
                service.wait_for_answer(subscription=mock_subscription, timeout=0.01)

    def test_error_raised_if_attempting_to_wait_for_answer_from_push_subscription(self):
        """Test that an error is raised if attempting to wait for an answer from a push subscription."""
        mock_subscription = MockSubscription(
            name="world",
            topic=MockTopic(name="world", project_name=TEST_PROJECT_NAME),
            project_name=TEST_PROJECT_NAME,
            push_endpoint="https://example.com/endpoint",
        )

        service = Service(backend=BACKEND)

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
            subscription, _ = parent.ask(service_id=child.id, input_values={})

            with self.assertRaises(twined.exceptions.InvalidManifestContents) as context:
                parent.wait_for_answer(subscription=subscription)

        self.assertIn("'met_mast_id' is a required property", context.exception.args[0])

    def test_exceptions_with_multiple_arguments_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions with multiple arguments raised in the child service are handled and sent back to
        the asker.
        """
        child = self.make_new_child_with_error(FileNotFoundError(2, "No such file or directory: 'blah'"))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            subscription, _ = parent.ask(service_id=child.id, input_values={})

            with self.assertRaises(FileNotFoundError) as context:
                parent.wait_for_answer(subscription)

        self.assertIn("[Errno 2] No such file or directory: 'blah'", format(context.exception))

    def test_unknown_exceptions_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions not in the exceptions mapping are simply raised as `Exception`s by the asker."""

        class AnUnknownException(Exception):
            pass

        child = self.make_new_child_with_error(AnUnknownException("This is an exception unknown to the asker."))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            subscription, _ = parent.ask(service_id=child.id, input_values={})

            with self.assertRaises(Exception) as context:
                parent.wait_for_answer(subscription)

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
                subscription, _ = parent.ask(service_id=child.id, input_values={}, subscribe_to_logs=False)
                answer = parent.wait_for_answer(subscription)

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
                subscription, _ = parent.ask(service_id=child.id, input_values={}, subscribe_to_logs=True)
                answer = parent.wait_for_answer(subscription, service_name="my-super-service")

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

        with self.assertLogs(level=logging.ERROR) as logs_context_manager:
            with self.service_patcher:
                child.serve()
                subscription, _ = parent.ask(service_id=child.id, input_values={}, subscribe_to_logs=True)
                parent.wait_for_answer(subscription, service_name="my-super-service")

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
            parent.wait_for_answer(subscription, handle_monitor_message=lambda data: monitoring_data.append(data))

        self.assertEqual(
            monitoring_data,
            [{"status": "my first monitor message"}, {"status": "my second monitor message"}],
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

        self.assertEqual(monitoring_data, [{"status": "my first monitor message"}])

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
                subscription, _ = parent.ask(service_id=child.id, input_values={}, input_manifest=input_manifest)
                answer = parent.wait_for_answer(subscription)

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
                subscription, _ = parent.ask(service_id=child.id, input_manifest=input_manifest)
                answer = parent.wait_for_answer(subscription)

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
                service_id="octue/test-service:latest",
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

            subscription, _ = parent.ask(
                service_id=child.id,
                input_values={},
                input_manifest=manifest,
                allow_local_files=True,
            )

            answer = parent.wait_for_answer(subscription)

        self.assertEqual(answer["output_values"], "This is a local file.")

    def test_ask_with_output_manifest(self):
        """Test that a service can receive an output manifest as part of the answer to a question."""
        child = self.make_new_child(BACKEND, run_function_returnee=MockAnalysisWithOutputManifest())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            subscription, _ = parent.ask(service_id=child.id, input_values={})
            answer = parent.wait_for_answer(subscription)

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
                subscription, _ = parent.ask(service_id=child.id, input_values={})
                answers.append(parent.wait_for_answer(subscription))

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

            subscription, _ = parent.ask(service_id=child_1.id, input_values={})
            answer_1 = parent.wait_for_answer(subscription)

            subscription, _ = parent.ask(service_id=child_2.id, input_values={})
            answer_2 = parent.wait_for_answer(subscription)

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

            subscription, _ = parent.ask(
                service_id=child.id,
                input_values={"question": "What does the child of the child say?"},
            )

            answer = parent.wait_for_answer(subscription)

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

            subscription, _ = parent.ask(
                service_id=child.id,
                input_values={"question": "What does the child of the child say?"},
            )

            answer = parent.wait_for_answer(subscription)

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

    def test_child_messages_can_be_recorded_by_parent(self):
        """Test that the parent can record messages it receives from its child to a JSON file."""
        child = MockService(backend=BACKEND, run_function=self.create_run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()
            subscription, _ = parent.ask(service_id=child.id, input_values={}, subscribe_to_logs=True)
            parent.wait_for_answer(subscription, service_name="my-super-service")

        # Check that the child's messages have been recorded by the parent.
        self.assertEqual(parent.recorded_messages[0]["type"], "delivery_acknowledgement")
        self.assertEqual(parent.recorded_messages[1]["type"], "log_record")
        self.assertEqual(parent.recorded_messages[2]["type"], "log_record")
        self.assertEqual(parent.recorded_messages[3]["type"], "log_record")

        self.assertEqual(
            parent.recorded_messages[4],
            {"type": "result", "output_values": "Hello! It worked!", "output_manifest": None, "message_number": 4},
        )

    def test_child_exception_message_can_be_recorded_by_parent(self):
        """Test that the parent can record exceptions raised by the child."""
        child = self.make_new_child_with_error(ValueError("Oh no."))
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with self.assertRaises(ValueError):
                subscription, _ = parent.ask(service_id=child.id, input_values={}, subscribe_to_logs=True)
                parent.wait_for_answer(subscription, service_name="my-super-service")

        # Check that the child's messages have been recorded by the parent.
        self.assertEqual(parent.recorded_messages[0]["type"], "delivery_acknowledgement")
        self.assertEqual(parent.recorded_messages[1]["type"], "exception")
        self.assertIn("Oh no.", parent.recorded_messages[1]["exception_message"])

    def test_child_sends_heartbeat_messages_at_expected_regular_intervals(self):
        """Test that children send heartbeat messages at the expected regular intervals."""
        expected_interval = 0.05

        def run_function(*args, **kwargs):
            time.sleep(0.3)
            return MockAnalysis()

        child = MockService(backend=BACKEND, run_function=lambda *args, **kwargs: run_function())
        parent = MockService(backend=BACKEND, children={child.id: child})

        with self.service_patcher:
            child.serve()

            with patch(
                "octue.cloud.emulators._pub_sub.MockService.answer",
                functools.partial(child.answer, heartbeat_interval=expected_interval),
            ):
                subscription, _ = parent.ask(
                    service_id=child.id,
                    input_values={},
                    subscribe_to_logs=True,
                    allow_save_diagnostics_data_on_crash=True,
                )

                parent.wait_for_answer(subscription, service_name="my-super-service")

        self.assertEqual(parent.recorded_messages[1]["type"], "heartbeat")
        self.assertEqual(parent.recorded_messages[2]["type"], "heartbeat")

        first_heartbeat_time = datetime.datetime.fromisoformat(parent.recorded_messages[1]["time"])
        second_heartbeat_time = datetime.datetime.fromisoformat(parent.recorded_messages[2]["time"])

        self.assertAlmostEqual(
            second_heartbeat_time - first_heartbeat_time,
            datetime.timedelta(seconds=expected_interval),
            delta=datetime.timedelta(0.05),
        )

    def test_providing_dynamic_children(self):
        """Test that, if children are provided to the `ask` method while asking a question, the child being asked uses
        those children instead of the children it has defined in its app configuration.
        """

        def mock_child_app(analysis):
            analysis.children["expected_child"]._service = child
            analysis.output_values = analysis.children["expected_child"].ask(input_values=[1, 2, 3, 4])["output_values"]

        static_children = [
            {
                "key": "expected_child",
                "id": "octue/static-child-of-child:latest",
                "backend": {"name": "GCPPubSubBackend", "project_name": "my-project"},
            },
        ]

        runner = Runner(
            app_src=mock_child_app,
            twine={
                "children": [{"key": "expected_child"}],
                "input_values_schema": {"type": "object", "required": []},
                "output_values_schema": {},
            },
            children=static_children,
            service_id="octue/child:latest",
        )

        static_child_of_child = self.make_new_child(
            backend=BACKEND,
            service_id="octue/static-child-of-child:latest",
            run_function_returnee=MockAnalysis(output_values="I am the static child."),
        )

        dynamic_child_of_child = self.make_new_child(
            backend=BACKEND,
            service_id="octue/dynamic-child-of-child:latest",
            run_function_returnee=MockAnalysis(output_values="I am the dynamic child."),
        )

        child = MockService(
            backend=BACKEND,
            service_id="octue/child:latest",
            run_function=runner.run,
            children={
                static_child_of_child.id: static_child_of_child,
                dynamic_child_of_child.id: dynamic_child_of_child,
            },
        )

        parent = MockService(backend=BACKEND, service_id="octue/parent:latest", children={child.id: child})

        dynamic_children = [
            {
                "key": "expected_child",
                "id": "octue/dynamic-child-of-child:latest",
                "backend": {"name": "GCPPubSubBackend", "project_name": "my-project"},
            },
        ]

        with self.service_patcher:
            static_child_of_child.serve()
            dynamic_child_of_child.serve()
            child.serve()

            subscription, _ = parent.ask(service_id=child.id, input_values={}, children=dynamic_children)
            answer = parent.wait_for_answer(subscription)

        self.assertEqual(answer["output_values"], "I am the dynamic child.")

    @staticmethod
    def make_new_child(backend, run_function_returnee, service_id=None):
        """Make and return a new child service that returns the given run function returnee when its run function is
        executed.

        :param octue.resources.service_backends.ServiceBackend backend:
        :param any run_function_returnee:
        :param str|None service_id:
        :return octue.cloud.emulators._pub_sub.MockService:
        """
        return MockService(
            backend=backend,
            service_id=service_id,
            run_function=lambda *args, **kwargs: run_function_returnee,
        )

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
