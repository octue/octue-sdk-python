import concurrent.futures
import time
import uuid
import google.api_core.exceptions

from octue import exceptions
from octue.resources.communication.google_pub_sub.service import OCTUE_NAMESPACE, Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from octue.resources.manifest import Manifest
from tests import TEST_PROJECT_NAME
from tests.base import BaseTestCase


SERVER_WAIT_TIME = 5


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
    def ask_question_and_wait_for_answer(asking_service, responding_service, input_values, input_manifest):
        """ Get an asking service to ask a question to a responding service and wait for the answer. """
        subscription = asking_service.ask(responding_service.id, input_values, input_manifest)
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
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        self.assertEqual(repr(asking_service), f"<Service({asking_service.name!r})>")

    def test_serve_with_timeout(self):
        """ Test that a serving service only serves for as long as its timeout. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            start_time = time.perf_counter()
            responding_future = executor.submit(
                responding_service.serve,
                timeout=10,
                delete_topic_and_subscription_on_exit=True,
            )

            responding_future.result()
            self.assertTrue(time.perf_counter() - start_time < 20)

        self._delete_topics_and_subscriptions(responding_service)

    def test_ask_on_non_existent_service_results_in_error(self):
        """Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error."""
        with self.assertRaises(exceptions.ServiceNotFound):
            Service(backend=self.BACKEND, id=str(uuid.uuid4())).ask(service_id=1234, input_values=[1, 2, 3, 4])

    def test_ask(self):
        """ Test that a service can ask a question to another service that is serving and receive an answer. """
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=None,
            )

            self.assertEqual(
                asker_future.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(responding_service)

    def test_ask_with_input_manifest(self):
        """Test that a service can ask a question including an input_manifest to another service that is serving and
        receive an answer.
        """
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=Manifest(),
            )

            self.assertEqual(
                asker_future.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(responding_service)

    def test_ask_with_output_manifest(self):
        """ Test that a service can receive an output manifest as part of the answer to a question. """
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysisWithOutputManifest())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=None,
            )

            answer = asker_future.result()
            self.assertEqual(answer["output_values"], MockAnalysisWithOutputManifest.output_values)
            self.assertEqual(answer["output_manifest"].id, MockAnalysisWithOutputManifest.output_manifest.id)
            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(responding_service)

    def test_service_can_ask_multiple_questions(self):
        """ Test that a service can ask multiple questions to the same server and expect replies to them all. """
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            executor.submit(responding_service.serve)
            futures = []

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            for i in range(5):
                futures.append(
                    executor.submit(
                        self.ask_question_and_wait_for_answer,
                        asking_service=asking_service,
                        responding_service=responding_service,
                        input_values={},
                        input_manifest=None,
                    )
                )

            self._shutdown_executor_and_clear_threads(executor)

        for answer in concurrent.futures.as_completed(futures):
            self.assertEqual(
                answer.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

        self._delete_topics_and_subscriptions(responding_service)

    def test_service_can_ask_questions_to_multiple_servers(self):
        """ Test that a service can ask questions to different servers and expect replies to them all. """
        asking_service = Service(backend=self.BACKEND, id=str(uuid.uuid4()))

        responding_service_1 = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())
        responding_service_2 = self.make_new_server(self.BACKEND, run_function_returnee=DifferentMockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            executor.submit(responding_service_1.serve)
            executor.submit(responding_service_2.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding services to be ready to answer.

            first_question_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service_1,
                input_values={},
                input_manifest=None,
            )

            second_question_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service_2,
                input_values={},
                input_manifest=None,
            )

            self.assertEqual(
                first_question_future.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

            self.assertEqual(
                second_question_future.result(),
                {
                    "output_values": DifferentMockAnalysis.output_values,
                    "output_manifest": DifferentMockAnalysis.output_manifest,
                },
            )

            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(responding_service_1, responding_service_2)

    def test_server_can_ask_its_own_child_questions(self):
        """Test that a child can contact its own child while answering a question from a parent."""
        child_of_child = self.make_new_server(
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), id=str(uuid.uuid4())
        )

        def child_run_function(input_values, input_manifest):
            service = Service(backend=self.BACKEND)
            subscription = service.ask(service_id=child_of_child.id, input_values=input_values)

            mock_analysis = MockAnalysis()
            mock_analysis.output_values = {input_values["question"]: service.wait_for_answer(subscription)}
            return mock_analysis

        parent = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        child = Service(backend=self.BACKEND, id=str(uuid.uuid4()), run_function=child_run_function)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.submit(child.serve)
            executor.submit(child_of_child.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding services to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=parent,
                responding_service=child,
                input_values={"question": "What does the child of the child say?"},
                input_manifest=None,
            )

            self.assertEqual(
                asker_future.result(),
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

            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(child, child_of_child)

    def test_server_can_ask_its_own_children_questions(self):
        """Test that a child can contact more than one of its own children while answering a question from a parent."""
        first_child_of_child = self.make_new_server(
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), id=str(uuid.uuid4())
        )

        second_child_of_child = self.make_new_server(
            self.BACKEND, run_function_returnee=MockAnalysis(), id=str(uuid.uuid4())
        )

        def child_run_function(input_values, input_manifest):
            child_service = Service(backend=self.BACKEND)
            subscription_1 = child_service.ask(service_id=first_child_of_child.id, input_values=input_values)
            subscription_2 = child_service.ask(service_id=second_child_of_child.id, input_values=input_values)

            mock_analysis = MockAnalysis()
            mock_analysis.output_values = {
                "first_child_of_child": child_service.wait_for_answer(subscription_1),
                "second_child_of_child": child_service.wait_for_answer(subscription_2),
            }

            return mock_analysis

        parent = Service(backend=self.BACKEND, id=str(uuid.uuid4()))
        child = Service(backend=self.BACKEND, id=str(uuid.uuid4()), run_function=child_run_function)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.submit(child.serve)
            executor.submit(first_child_of_child.serve)
            executor.submit(second_child_of_child.serve)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding services to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=parent,
                responding_service=child,
                input_values={"question": "What does the child of the child say?"},
                input_manifest=None,
            )

            self.assertEqual(
                asker_future.result(),
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

            self._shutdown_executor_and_clear_threads(executor)

        self._delete_topics_and_subscriptions(child, first_child_of_child, second_child_of_child)
