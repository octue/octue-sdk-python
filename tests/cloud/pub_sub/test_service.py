import tempfile
import uuid
from unittest.mock import patch

import twined.exceptions
from octue import Runner, exceptions
from octue.cloud.pub_sub.service import Service
from octue.exceptions import InvalidMonitorMessage
from octue.resources import Datafile, Dataset, Manifest
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import (
    DifferentMockAnalysis,
    MockAnalysis,
    MockAnalysisWithOutputManifest,
    MockPullResponse,
    MockService,
    MockSubscriber,
    MockSubscription,
    MockTopic,
)


BACKEND = GCPPubSubBackend(
    project_name=TEST_PROJECT_NAME, credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS"
)


def create_run_function():
    """Create a run function that sends log messages back to the parent and gives a simple output value.

    :return callable: the run function
    """

    def mock_app(analysis):
        analysis.logger.info("Starting analysis.")
        analysis.output_values = "Hello! It worked!"
        analysis.output_manifest = None
        analysis.logger.info("Finished analysis.")

    twine = """
        {
            "input_values_schema": {
                "type": "object",
                "required": []
            }
        }
    """

    return Runner(app_src=mock_app, twine=twine).run


class TestService(BaseTestCase):
    """Some of these tests require a connection to either a real Google Pub/Sub instance on Google Cloud Platform
    (GCP), or a local emulator.
    """

    @staticmethod
    def make_new_server(backend, run_function_returnee, use_mock=False):
        """Make and return a new service that returns the given run function returnee when its run function is executed.

        :param octue.resources.service_backends.ServiceBackend backend:
        :param any run_function_returnee:
        :param bool use_mock:
        :return tests.cloud.pub_sub.mocks.MockService:
        """
        run_function = (
            lambda analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message: run_function_returnee
        )

        if use_mock:
            return MockService(backend=backend, run_function=run_function)
        return Service(backend=backend, run_function=run_function)

    @staticmethod
    def ask_question_and_wait_for_answer(
        asking_service,
        responding_service,
        input_values,
        input_manifest,
        subscribe_to_logs=True,
        allow_local_files=False,
        service_name="my-service",
        timeout=30,
        delivery_acknowledgement_timeout=30,
    ):
        """Get an asking service to ask a question to a responding service and wait for the answer.

        :param tests.cloud.pub_sub.mocks.MockService asking_service:
        :param tests.cloud.pub_sub.mocks.MockService responding_service:
        :param dict|None input_values:
        :param octue.resources.manifest.Manifest|None input_manifest:
        :param bool subscribe_to_logs:
        :return dict:
        """
        subscription, _ = asking_service.ask(
            responding_service.id, input_values, input_manifest, subscribe_to_logs, allow_local_files, timeout
        )

        return asking_service.wait_for_answer(
            subscription, service_name=service_name, delivery_acknowledgement_timeout=delivery_acknowledgement_timeout
        )

    def make_responding_service_with_error(self, exception_to_raise):
        """Make a mock responding service that raises the given exception when its run function is executed.

        :param Exception exception_to_raise:
        :return tests.cloud.pub_sub.mocks.MockService:
        """
        responding_service = self.make_new_server(BACKEND, run_function_returnee=None, use_mock=True)

        def error_run_function(analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
            raise exception_to_raise

        responding_service.run_function = error_run_function
        return responding_service

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
        asking_service = Service(backend=BACKEND)
        self.assertEqual(repr(asking_service), f"<Service({asking_service.name!r})>")

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

    def test_exceptions_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions raised in the responding service are handled and sent back to the asker."""
        responding_service = self.make_responding_service_with_error(
            twined.exceptions.InvalidManifestContents("'met_mast_id' is a required property")
        )

        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    with self.assertRaises(twined.exceptions.InvalidManifestContents) as context:
                        self.ask_question_and_wait_for_answer(
                            asking_service=asking_service,
                            responding_service=responding_service,
                            input_values={},
                            input_manifest=None,
                        )

        self.assertIn("'met_mast_id' is a required property", context.exception.args[0])

    def test_exceptions_with_multiple_arguments_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions with multiple arguments raised in the responding service are handled and sent back to
        the asker.
        """
        responding_service = self.make_responding_service_with_error(
            FileNotFoundError(2, "No such file or directory: 'blah'")
        )

        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    with self.assertRaises(FileNotFoundError) as context:
                        self.ask_question_and_wait_for_answer(
                            asking_service=asking_service,
                            responding_service=responding_service,
                            input_values={},
                            input_manifest=None,
                        )

        self.assertIn("[Errno 2] No such file or directory: 'blah'", format(context.exception))

    def test_unknown_exceptions_in_responder_are_handled_and_sent_to_asker(self):
        """Test that exceptions not in the exceptions mapping are simply raised as `Exception`s by the asker."""

        class AnUnknownException(Exception):
            pass

        responding_service = self.make_responding_service_with_error(
            AnUnknownException("This is an exception unknown to the asker.")
        )

        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    with self.assertRaises(Exception) as context:
                        self.ask_question_and_wait_for_answer(
                            asking_service=asking_service,
                            responding_service=responding_service,
                            input_values={},
                            input_manifest=None,
                        )

        self.assertEqual(type(context.exception).__name__, "AnUnknownException")
        self.assertIn("This is an exception unknown to the asker.", context.exception.args[0])

    def test_question_is_retried_if_delivery_acknowledgement_not_received(self):
        """Test that a question is asked again if delivery is not acknowledged and that the re-asked question is then
        processed.
        """
        responding_service = self.make_new_server(backend=BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    # Stop the responding service from answering.
                    with patch("octue.cloud.pub_sub.service.Service.answer"):
                        subscription, _ = asking_service.ask(service_id=responding_service.id, input_values={})

                    # Wait for an answer and check that the question is asked again.
                    with self.assertLogs() as logging_context:
                        answer = asking_service.wait_for_answer(
                            subscription,
                            delivery_acknowledgement_timeout=0.01,
                            retry_interval=0.1,
                        )

                        self.assertTrue(
                            any(
                                "No acknowledgement of question delivery" in log_message
                                for log_message in logging_context.output
                            )
                        )

        self.assertEqual(answer, {"output_values": "Hello! It worked!", "output_manifest": None})

    def test_ask_with_real_run_function_with_no_log_message_forwarding(self):
        """Test that a service can ask a question to another service that is serving and receive an answer. Use a real
        run function rather than a mock so that the underlying `Runner` instance is used, and check that remote log
        messages aren't forwarded to the local logger.
        """
        responding_service = MockService(backend=BACKEND, run_function=create_run_function())
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with self.assertLogs() as logging_context:
            with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                    with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                        responding_service.serve()

                        answer = self.ask_question_and_wait_for_answer(
                            asking_service=asking_service,
                            responding_service=responding_service,
                            input_values={},
                            input_manifest=None,
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
        responding_service = MockService(backend=BACKEND, run_function=create_run_function())
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with self.assertLogs() as logs_context_manager:
            with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                    with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                        responding_service.serve()

                        answer = self.ask_question_and_wait_for_answer(
                            asking_service=asking_service,
                            responding_service=responding_service,
                            input_values={},
                            input_manifest=None,
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
            if log_record.msg == "[my-super-service] Starting analysis.":
                start_remote_analysis_message_present = True

                if logs_context_manager.records[i + 1].msg == "[my-super-service] Finished analysis.":
                    finish_remote_analysis_message_present = True

                break

        self.assertTrue(start_remote_analysis_message_present)
        self.assertTrue(finish_remote_analysis_message_present)

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

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    child.serve()

                    subscription, _ = parent.ask(child.id, input_values={})

                    monitoring_data = []
                    parent.wait_for_answer(
                        subscription, handle_monitor_message=lambda data: monitoring_data.append(data)
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

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
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
        """Test that a service can ask a question including an input_manifest to another service that is serving and
        receive an answer.
        """
        responding_service = self.make_new_server(BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        files = [
            Datafile(path="gs://my-dataset/hello.txt", project_name="blah", hypothetical=True),
            Datafile(path="gs://my-dataset/goodbye.csv", project_name="blah", hypothetical=True),
        ]

        input_manifest = Manifest(datasets=[Dataset(files=files)], path="gs://my-dataset", keys={"my_dataset": 0})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=asking_service,
                        responding_service=responding_service,
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
        responding_service = self.make_new_server(BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        files = [
            Datafile(path="gs://my-dataset/hello.txt", project_name=TEST_PROJECT_NAME, hypothetical=True),
            Datafile(path="gs://my-dataset/goodbye.csv", project_name=TEST_PROJECT_NAME, hypothetical=True),
        ]

        input_manifest = Manifest(datasets=[Dataset(files=files)], path="gs://my-dataset", keys={"my_dataset": 0})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=asking_service,
                        responding_service=responding_service,
                        input_values=None,
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
                input_manifest=Manifest(),
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

        manifest = Manifest(datasets=[Dataset(name="my-local-dataset", file=local_file)], keys={0: "my-local-dataset"})

        # Get the child to open the local file itself and return the contents as output.
        def run_function(analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
            with open(temporary_local_path) as f:
                return MockAnalysis(output_values=f.read())

        child = MockService(backend=BACKEND, run_function=run_function)
        parent = MockService(backend=BACKEND, children={child.id: child})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    child.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=parent,
                        responding_service=child,
                        input_values={},
                        input_manifest=manifest,
                        allow_local_files=True,
                    )

        self.assertEqual(answer["output_values"], "This is a local file.")

    def test_ask_with_output_manifest(self):
        """Test that a service can receive an output manifest as part of the answer to a question."""
        responding_service = self.make_new_server(
            BACKEND, run_function_returnee=MockAnalysisWithOutputManifest(), use_mock=True
        )
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=asking_service,
                        responding_service=responding_service,
                        input_values={},
                        input_manifest=None,
                    )

        self.assertEqual(answer["output_values"], MockAnalysisWithOutputManifest.output_values)
        self.assertEqual(answer["output_manifest"].id, MockAnalysisWithOutputManifest.output_manifest.id)

    def test_service_can_ask_multiple_questions(self):
        """Test that a service can ask multiple questions to the same server and expect replies to them all."""
        responding_service = self.make_new_server(BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=BACKEND, children={responding_service.id: responding_service})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service.serve()

                    answers = []

                    for i in range(5):
                        answers.append(
                            self.ask_question_and_wait_for_answer(
                                asking_service=asking_service,
                                responding_service=responding_service,
                                input_values={},
                                input_manifest=None,
                            )
                        )

        for answer in answers:
            self.assertEqual(
                answer,
                {"output_values": MockAnalysis().output_values, "output_manifest": MockAnalysis().output_manifest},
            )

    def test_service_can_ask_questions_to_multiple_servers(self):
        """Test that a service can ask questions to different servers and expect replies to them all."""
        responding_service_1 = self.make_new_server(BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        responding_service_2 = self.make_new_server(
            BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True
        )

        asking_service = MockService(
            backend=BACKEND,
            children={responding_service_1.id: responding_service_1, responding_service_2.id: responding_service_2},
        )

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    responding_service_1.serve()
                    responding_service_2.serve()

                    answer_1 = self.ask_question_and_wait_for_answer(
                        asking_service=asking_service,
                        responding_service=responding_service_1,
                        input_values={},
                        input_manifest=None,
                    )

                    answer_2 = self.ask_question_and_wait_for_answer(
                        asking_service=asking_service,
                        responding_service=responding_service_2,
                        input_values={},
                        input_manifest=None,
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

    def test_server_can_ask_its_own_child_questions(self):
        """Test that a child can contact its own child while answering a question from a parent."""
        child_of_child = self.make_new_server(BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True)

        def child_run_function(analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
            subscription, _ = child.ask(service_id=child_of_child.id, input_values=input_values)
            return MockAnalysis(output_values={input_values["question"]: child.wait_for_answer(subscription)})

        child = MockService(
            backend=BACKEND, run_function=child_run_function, children={child_of_child.id: child_of_child}
        )

        parent = MockService(backend=BACKEND, children={child.id: child})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    child.serve()
                    child_of_child.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=parent,
                        responding_service=child,
                        input_values={"question": "What does the child of the child say?"},
                        input_manifest=None,
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

    def test_server_can_ask_its_own_children_questions(self):
        """Test that a child can contact more than one of its own children while answering a question from a parent."""
        first_child_of_child = self.make_new_server(
            BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True
        )
        second_child_of_child = self.make_new_server(BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)

        def child_run_function(analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
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

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                    child.serve()
                    first_child_of_child.serve()
                    second_child_of_child.serve()

                    answer = self.ask_question_and_wait_for_answer(
                        asking_service=parent,
                        responding_service=child,
                        input_values={"question": "What does the child of the child say?"},
                        input_manifest=None,
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
