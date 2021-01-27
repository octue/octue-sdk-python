import concurrent.futures
import time
from tests.base import BaseTestCase

from octue import exceptions
from octue.resources.manifest import Manifest
from octue.resources.service import Service
from octue.resources.service_backends import GCPPubSubBackend


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
    """ Some of these tests require a connection to either a real Google Pub/Sub instance on Google Cloud Platform
    (GCP), or a local emulator. """

    BACKEND = GCPPubSubBackend(
        project_name="octue-amy",
        credentials_filename="/Users/Marcus1/repos/octue-sdk-python/octue-amy-670f6026b822.json",
    )
    asking_service = Service(backend=BACKEND, id="249fc09d-9d6f-45d6-b1a4-0aacba5fca79")

    @staticmethod
    def ask_question_and_wait_for_answer(asking_service, responding_service, input_values, input_manifest):
        """ Get an asking service to ask a question to a responding service and wait for the answer. """
        subscription = asking_service.ask(responding_service.id, input_values, input_manifest)
        return asking_service.wait_for_answer(subscription)

    def test_repr(self):
        """ Test that services are represented as a string correctly. """
        self.assertEqual(repr(self.asking_service), f"<Service({self.asking_service.name!r})>")

    def test_serve_with_timeout(self):
        """ Test that a serving service only serves for as long as its timeout. """
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            start_time = time.perf_counter()
            responding_future = executor.submit(
                self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis()).serve, timeout=10
            )

            responding_future.result()
            self.assertTrue(time.perf_counter() - start_time < 20)

    def test_ask_on_non_existent_service_results_in_error(self):
        """ Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error. """
        with self.assertRaises(exceptions.ServiceNotFound):
            Service(backend=self.BACKEND, id="249fc09d-9d6f-45d6-b1a4-0aacba5fca79").ask(
                service_id=1234, input_values=[1, 2, 3, 4]
            )

    def test_ask(self):
        """ Test that a service can ask a question to another service that is serving and receive an answer. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=None,
            )

            self.assertEqual(
                asker_future.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

    def test_ask_with_input_manifest(self):
        """ Test that a service can ask a question including an input_manifest to another service that is serving and
        receive an answer.
        """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=Manifest(),
            )

            self.assertEqual(
                asker_future.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

    def test_ask_with_output_manifest(self):
        """ Test that a service can receive an output manifest as part of the answer to a question. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysisWithOutputManifest())

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=None,
            )

            answer = asker_future.result()
            self.assertEqual(answer["output_values"], MockAnalysisWithOutputManifest.output_values)
            self.assertEqual(answer["output_manifest"].id, MockAnalysisWithOutputManifest.output_manifest.id)

    def test_service_can_ask_multiple_questions(self):
        """ Test that a service can ask multiple questions to the same server and expect replies to them all. """
        responding_service = self.make_new_server(self.BACKEND, run_function_returnee=MockAnalysis())

        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            executor.submit(responding_service.serve, timeout=10)
            futures = []

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding service to be ready to answer.

            for i in range(5):
                futures.append(
                    executor.submit(
                        self.ask_question_and_wait_for_answer,
                        asking_service=self.asking_service,
                        responding_service=responding_service,
                        input_values={},
                        input_manifest=None,
                    )
                )

        answers = list(concurrent.futures.as_completed(futures))

        for answer in answers:
            self.assertEqual(
                answer.result(),
                {"output_values": MockAnalysis.output_values, "output_manifest": MockAnalysis.output_manifest},
            )

    def test_service_can_ask_questions_to_multiple_servers(self):
        """ Test that a service can ask questions to different servers and expect replies to them all. """
        responding_service_1 = self.make_new_server(
            self.BACKEND, run_function_returnee=MockAnalysis(), id="352f8185-1d58-4ddf-8faa-2af96147f96f",
        )

        responding_service_2 = self.make_new_server(
            self.BACKEND, run_function_returnee=DifferentMockAnalysis(), id="6a05b695-4aa1-468c-bdf5-73303665165d",
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            executor.submit(responding_service_1.serve, timeout=10)
            executor.submit(responding_service_2.serve, timeout=10)

            time.sleep(SERVER_WAIT_TIME)  # Wait for the responding services to be ready to answer.

            first_question_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service_1,
                input_values={},
                input_manifest=None,
            )

            second_question_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
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
