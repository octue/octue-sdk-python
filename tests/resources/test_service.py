import concurrent.futures
import time
from tests.base import BaseTestCase

from octue import exceptions
from octue.resources.manifest import Manifest
from octue.resources.service import Service


class FakeAnalysis:
    output_values = "Hello! It worked!"


class TestService(BaseTestCase):
    """ Some of these tests require a connection to either a real Google Pub/Sub instance on Google Cloud Platform
    (GCP), or a local emulator. """

    GCP_PROJECT = "octue-amy"
    asking_service = Service(name="asker", gcp_project_name=GCP_PROJECT, id="249fc09d-9d6f-45d6-b1a4-0aacba5fca79")

    def make_new_server(self):
        return Service(
            name="server",
            gcp_project_name=self.GCP_PROJECT,
            id="352f8185-1d58-4ddf-8faa-2af96147f96f",
            run_function=lambda input_values, input_manifest: FakeAnalysis(),
        )

    def ask_question_and_wait_for_answer(self, asking_service, responding_service, input_values, input_manifest):
        """ Get an asking service to ask a question to a responding service and wait for the answer. """
        time.sleep(5)  # Wait for the responding service to be ready to answer.
        subscription = asking_service.ask(responding_service.id, input_values, input_manifest)
        return asking_service.wait_for_answer(subscription)

    def test_serve_with_timeout(self):
        """ Test that a serving service only serves for as long as its timeout. """
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            start_time = time.perf_counter()
            responding_future = executor.submit(self.make_new_server().serve, timeout=10)
            list(concurrent.futures.as_completed([responding_future]))[0].result()
            self.assertTrue(time.perf_counter() - start_time < 15)

    def test_ask_on_non_existent_service_results_in_error(self):
        """ Test that trying to ask a question to a non-existent service (i.e. one without a topic in Google Pub/Sub)
        results in an error. """
        with self.assertRaises(exceptions.ServiceNotFound):
            Service(name="asker", gcp_project_name=self.GCP_PROJECT, id="249fc09d-9d6f-45d6-b1a4-0aacba5fca79").ask(
                service_id=1234, input_values=[1, 2, 3, 4]
            )

    def test_ask(self):
        """ Test that a service can ask a question to another service that is serving and receive an answer. """
        responding_service = self.make_new_server()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=None,
            )

            answer = list(concurrent.futures.as_completed([asker_future]))[0].result()
            self.assertEqual(answer, FakeAnalysis.output_values)

    def test_ask_with_input_manifest(self):
        """ Test that a service can ask a question including an input_manifest to another service that is serving and
        receive an answer.
        """
        responding_service = self.make_new_server()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=self.asking_service,
                responding_service=responding_service,
                input_values={},
                input_manifest=Manifest(),
            )

            answer = list(concurrent.futures.as_completed([asker_future]))[0].result()
            self.assertEqual(answer, FakeAnalysis.output_values)
