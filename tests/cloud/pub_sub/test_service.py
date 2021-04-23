import concurrent.futures
import uuid
from unittest.mock import patch
import google.api_core.exceptions

from octue import exceptions
from octue.cloud.pub_sub.service import OCTUE_NAMESPACE, Service
from octue.resources import Datafile, Dataset, Manifest
from octue.resources.service_backends import GCPPubSubBackend
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mock_service import MockService, MockSubscription, MockTopic


class MockAnalysis:
    output_values = "Hello! It worked!"
    output_manifest = None


class DifferentMockAnalysis:
    output_values = "This is another successful analysis."
    output_manifest = None


class MockAnalysisWithOutputManifest:
    output_values = "This is an analysis with an empty output manifest."
    output_manifest = Manifest()


class TestService(BaseTestCase):
    """Some of these tests require a connection to either a real Google Pub/Sub instance on Google Cloud Platform
    (GCP), or a local emulator."""

    BACKEND = GCPPubSubBackend(
        project_name=TEST_PROJECT_NAME, credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS"
    )

    @staticmethod
    def make_new_server(backend, run_function_returnee, use_mock=False):
        """ Make and return a new service ready to serve analyses from its run function. """
        run_function = lambda input_values, input_manifest: run_function_returnee  # noqa

        if use_mock:
            return MockService(backend=backend, run_function=run_function)
        return Service(backend=backend, run_function=run_function)

    @staticmethod
    def ask_question_and_wait_for_answer(asking_service, responding_service, input_values, input_manifest):
        """ Get an asking service to ask a question to a responding service and wait for the answer. """
        subscription, _ = asking_service.ask(responding_service.id, input_values, input_manifest)
        return asking_service.wait_for_answer(subscription)

    @staticmethod
    def _delete_topics_and_subscriptions(*services):
        """Delete the topics and subscriptions created for each service. This method is necessary as it is difficult to
        end the threads running serving services gracefully, which would delete the topics and subscriptions if asked.
        """
        for service in services:
            topic_path = service.publisher.topic_path(service.backend.project_name, f"{OCTUE_NAMESPACE}.{service.id}")
            try:
                service.publisher.delete_topic(topic=topic_path)
            except google.api_core.exceptions.NotFound:
                pass

            subscription_path = service.subscriber.subscription_path(
                service.backend.project_name, f"{OCTUE_NAMESPACE}.{service.id}"
            )

            try:
                service.subscriber.delete_subscription(subscription=subscription_path)
            except (google.api_core.exceptions.NotFound, ValueError):
                pass

    def _shutdown_executor_and_clear_threads(self, executor):
        """Shut down a concurrent.futures executor by clearing its threads. This cuts loose infinitely-waiting threads,
        allowing the test to finish.
        """
        executor._threads.clear()
        executor.shutdown()
        concurrent.futures.thread._threads_queues.clear()

    def test_repr(self):
        """ Test that services are represented as a string correctly. """
        asking_service = Service(backend=self.BACKEND)
        self.assertEqual(repr(asking_service), f"<Service({asking_service.name!r})>")

    def test_ask_on_non_existent_service_results_in_error(self):
        """Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error."""
        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with self.assertRaises(exceptions.ServiceNotFound):
                MockService(backend=self.BACKEND).ask(service_id="hello", input_values=[1, 2, 3, 4])

    def test_ask(self):
        """ Test that a service can ask a question to another service that is serving and receive an answer. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=self.BACKEND, children=[responding_service])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                responding_service.serve()

                answer = self.ask_question_and_wait_for_answer(
                    asking_service=asking_service,
                    responding_service=responding_service,
                    input_values={},
                    input_manifest=None,
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
        )

    def test_ask_with_input_manifest(self):
        """Test that a service can ask a question including an input_manifest to another service that is serving and
        receive an answer.
        """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=self.BACKEND, children=[responding_service])

        files = [
            Datafile(timestamp=None, path="gs://my-dataset/hello.txt"),
            Datafile(timestamp=None, path="gs://my-dataset/goodbye.csv"),
        ]

        input_manifest = Manifest(datasets=[Dataset(files=files)], path="gs://my-dataset", keys={"my_dataset": 0})

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                responding_service.serve()

                answer = self.ask_question_and_wait_for_answer(
                    asking_service=asking_service,
                    responding_service=responding_service,
                    input_values={},
                    input_manifest=input_manifest,
                )

        self.assertEqual(
            answer,
            {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
        )

    def test_ask_with_input_manifest_with_local_paths_raises_error(self):
        """Test that an error is raised if an input manifest whose datasets and/or files are not located in the cloud
        is used in a question.
        """
        with self.assertRaises(exceptions.FileLocationError):
            MockService(backend=self.BACKEND).ask(
                service_id=str(uuid.uuid4()),
                input_values={},
                input_manifest=Manifest(),
            )

    def test_ask_with_output_manifest(self):
        """ Test that a service can receive an output manifest as part of the answer to a question. """
        responding_service = self.make_new_server(
            self.BACKEND, run_function_returnee=MockAnalysisWithOutputManifest(), use_mock=True
        )
        asking_service = MockService(backend=self.BACKEND, children=[responding_service])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
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
        """ Test that a service can ask multiple questions to the same server and expect replies to them all. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        asking_service = MockService(backend=self.BACKEND, children=[responding_service])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
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
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

    def test_service_can_ask_questions_to_multiple_servers(self):
        """ Test that a service can ask questions to different servers and expect replies to them all. """
        responding_service_1 = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)
        responding_service_2 = self.make_new_server(
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True
        )

        asking_service = MockService(backend=self.BACKEND, children=[responding_service_1, responding_service_2])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                responding_service_1.serve()
                responding_service_2.serve()

                first_question_future = self.ask_question_and_wait_for_answer(
                    asking_service=asking_service,
                    responding_service=responding_service_1,
                    input_values={},
                    input_manifest=None,
                )

                second_question_future = self.ask_question_and_wait_for_answer(
                    asking_service=asking_service,
                    responding_service=responding_service_2,
                    input_values={},
                    input_manifest=None,
                )

        self.assertEqual(
            first_question_future,
            {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
        )

        self.assertEqual(
            second_question_future,
            {
                "output_values": DifferentMockAnalysis.output_values,
                "output_manifest": DifferentMockAnalysis.output_manifest,
            },
        )

    def test_server_can_ask_its_own_child_questions(self):
        """Test that a child can contact its own child while answering a question from a parent."""
        child_of_child = self.make_new_server(
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True
        )

        def child_run_function(input_values, input_manifest):
            subscription, _ = child.ask(service_id=child_of_child.id, input_values=input_values)
            mock_analysis = MockAnalysis()
            mock_analysis.output_values = {input_values["question"]: child.wait_for_answer(subscription)}
            return mock_analysis

        child = MockService(backend=self.BACKEND, run_function=child_run_function, children=[child_of_child])
        parent = MockService(backend=self.BACKEND, children=[child])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
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
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), use_mock=True
        )
        second_child_of_child = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis(), use_mock=True)

        def child_run_function(input_values, input_manifest):
            subscription_1, _ = child.ask(service_id=first_child_of_child.id, input_values=input_values)
            subscription_2, _ = child.ask(service_id=second_child_of_child.id, input_values=input_values)

            mock_analysis = MockAnalysis()
            mock_analysis.output_values = {
                "first_child_of_child": child.wait_for_answer(subscription_1),
                "second_child_of_child": child.wait_for_answer(subscription_2),
            }

            return mock_analysis

        child = MockService(
            backend=self.BACKEND,
            run_function=child_run_function,
            children=[first_child_of_child, second_child_of_child],
        )
        parent = MockService(backend=self.BACKEND, children=[child])

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
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
                        "output_values": MockAnalysis.output_values,
                        "output_manifest": MockAnalysis.output_manifest,
                    },
                },
                "output_manifest": None,
            },
        )
