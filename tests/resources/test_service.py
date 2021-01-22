import concurrent.futures
import time
from tests.base import BaseTestCase

from octue.resources.service import Service


class FakeAnalysis:
    output_values = "Hello! It worked!"


class TestService(BaseTestCase):

    GCP_PROJECT = "octue-amy"

    def ask_question_and_wait_for_answer(self, asking_service, responding_service, input_values):
        """ Get an asking service to ask a question to a responding service and wait for the answer."""
        time.sleep(5)  # Wait for the responding service to be ready to answer.
        subscription = asking_service.ask(service_id=responding_service.id, input_values=input_values)
        return asking_service.wait_for_answer(subscription)

    def test_ask(self):
        """ Test that a service can ask a question to another service that is serving and receive an answer. """
        asking_service = Service(
            name="asker", gcp_project_name=self.GCP_PROJECT, id="249fc09d-9d6f-45d6-b1a4-0aacba5fca79"
        )

        responding_service = Service(
            name="server",
            gcp_project_name=self.GCP_PROJECT,
            id="352f8185-1d58-4ddf-8faa-2af96147f96f",
            run_function=lambda x: FakeAnalysis(),
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(responding_service.serve, timeout=10)

            asker_future = executor.submit(
                self.ask_question_and_wait_for_answer,
                asking_service=asking_service,
                responding_service=responding_service,
                input_values={},
            )

            answer = list(concurrent.futures.as_completed([asker_future]))[0].result()
            self.assertEqual(answer, FakeAnalysis.output_values)
