import functools
import logging
from multiprocessing import Value
import os
import random
import threading
import time
from unittest.mock import patch

from octue.twined.cloud.emulators._pub_sub import MockAnalysis, MockService
from octue.twined.cloud.emulators.service import ServicePatcher
from octue.twined.resources.child import Child
from octue.twined.resources.service_backends import GCPPubSubBackend
from tests import MOCK_SERVICE_REVISION_TAG
from tests.base import BaseTestCase

lock = threading.Lock()


def mock_run_function_that_fails(analysis_id, input_values, *args, **kwargs):
    """A run function that always fails."""
    raise ValueError("Deliberately raised for `Child.ask` test.")


def mock_run_function_that_fails_every_other_time(analysis_id, input_values, *args, **kwargs):
    """A run function that always fails every other time, starting with the first time."""
    with lock:
        # Every other question will fail.
        if kwargs["runs"].value % 2 == 0:
            kwargs["runs"].value += 1
            raise ValueError("Deliberately raised for `Child.ask` test.")

    time.sleep(random.random() * 0.1)
    return MockAnalysis(output_values=input_values)


class TestChild(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        """Start the service patcher.

        :return None:
        """
        cls.service_patcher.start()

    @classmethod
    def tearDownClass(cls):
        """Stop the service patcher.

        :return None:
        """
        cls.service_patcher.stop()

    def test_representation(self):
        """Test that children are represented correctly as a string."""
        self.assertEqual(
            repr(
                Child(
                    id=f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}",
                    backend={"name": "GCPPubSubBackend", "project_id": "blah"},
                )
            ),
            f"<Child('octue/my-child:{MOCK_SERVICE_REVISION_TAG}')>",
        )

    def test_instantiating_child_without_credentials(self):
        """Test that a child can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Child(
                id=f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}",
                backend={"name": "GCPPubSubBackend", "project_id": "blah"},
            )

    def test_child_can_be_asked_multiple_questions_in_serial(self):
        """Test that a child can be asked multiple questions in serial."""

        def mock_run_function(analysis_id, input_values, *args, **kwargs):
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(backend=GCPPubSubBackend(project_id="blah"), run_function=mock_run_function)

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()
            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service
            self.assertEqual(child.ask([1, 2, 3, 4])[0]["output_values"], [1, 2, 3, 4])
            self.assertEqual(child.ask([5, 6, 7, 8])[0]["output_values"], [5, 6, 7, 8])

    def test_error_raised_if_question_fails_when_raise_errors_is_true(self):
        """Test that an error is raised if the question fails when `raise_errors` is `True`."""
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=mock_run_function_that_fails,
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()

            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service

            with self.assertRaises(ValueError):
                child.ask(input_values=[1, 2, 3, 4])

    def test_error_not_raised_if_question_fails_when_raise_errors_is_false(self):
        """Test that an error is not raised if the question fails when `raise_errors` is `False`."""
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=mock_run_function_that_fails,
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()

            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service
            answer, _ = child.ask(input_values=[1, 2, 3, 4], raise_errors=False)

        self.assertTrue(isinstance(answer, ValueError))
        self.assertIn("Deliberately raised for `Child.ask` test.", answer.args[0])

    def test_with_failed_question_retry(self):
        """Test that a failed question can be automatically retried."""
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=functools.partial(mock_run_function_that_fails_every_other_time, runs=Value("d", 0)),
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()
            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service
            answer, _ = child.ask(input_values=[1, 2, 3, 4], raise_errors=False, max_retries=1)

        # Check that the question succeeds.
        self.assertEqual(answer, {"output_manifest": None, "output_values": [1, 2, 3, 4]})

    def test_errors_logged_when_not_raised(self):
        """Test that errors from a question still failing after retries are exhausted are logged by default."""
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=functools.partial(mock_run_function_that_fails_every_other_time, runs=Value("d", 0)),
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()
            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service

            with self.assertLogs(level=logging.ERROR) as logging_context:
                child.ask(input_values=[1, 2, 3, 4], raise_errors=False, max_retries=0)

            self.assertIn("failed after 0 retries (see below for error).", logging_context.output[2])
            self.assertIn('raise ValueError("Deliberately raised for `Child.ask` test.")', logging_context.output[2])

    def test_with_prevented_retries(self):
        """Test that retries can be prevented for specified exception types."""
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=functools.partial(mock_run_function_that_fails_every_other_time, runs=Value("d", 0)),
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()

            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service

            with self.assertLogs() as logging_context:
                answer, _ = child.ask(
                    input_values=[1, 2, 3, 4],
                    raise_errors=False,
                    max_retries=1,
                    prevent_retries_when=[ValueError],
                )

        self.assertTrue(isinstance(answer, ValueError))
        self.assertIn("Deliberately raised for `Child.ask` test.", answer.args[0])
        self.assertIn("Skipping retries for exceptions of type <class 'ValueError'>.", logging_context.output[-1])


class TestAskMultiple(BaseTestCase):
    service_patcher = ServicePatcher()

    @classmethod
    def setUpClass(cls):
        """Start the service patcher.

        :return None:
        """
        cls.service_patcher.start()

    @classmethod
    def tearDownClass(cls):
        """Stop the service patcher.

        :return None:
        """
        cls.service_patcher.stop()

    def test_ask_multiple(self):
        """Test that a child can be asked multiple questions in parallel and return the answers in the correct order."""

        def mock_run_function(analysis_id, input_values, *args, **kwargs):
            time.sleep(random.randint(0, 2))
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(backend=GCPPubSubBackend(project_id="blah"), run_function=mock_run_function)

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()

            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service

            answers = child.ask_multiple(
                {"input_values": [1, 2, 3, 4]},
                {"input_values": [5, 6, 7, 8]},
            )

            self.assertEqual(
                [answer[0] for answer in answers],
                [
                    {"output_values": [1, 2, 3, 4], "output_manifest": None},
                    {"output_values": [5, 6, 7, 8], "output_manifest": None},
                ],
            )

    def test_with_multiple_failed_question_retries(self):
        """Test that repeatedly failed questions can be automatically retried more than once. We use a lock in the run
        function so that the questions always succeed/fail in this order (which is the order the questions end up being
        asked by the thread pool, not necessarily the order they're asked by the caller of `Child.ask_multiple`):
        1. First question succeeds
        2. Second question fails
        3. Third question succeeds
        4. Fourth question fails
        5. Second question is retried and succeeds
        6. Fourth question is retried and fails
        7. Fourth question is retried again and succeeds
        """
        responding_service = MockService(
            backend=GCPPubSubBackend(project_id="blah"),
            run_function=functools.partial(mock_run_function_that_fails_every_other_time, runs=Value("d", 0)),
        )

        with patch("octue.twined.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
            responding_service.serve()

            child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_id": "blah"})

            # Make sure the child's underlying mock service knows how to access the mock responding service.
            child._service.children[responding_service.id] = responding_service

            # Only ask two questions so the question success/failure order plays out as desired.
            answers = child.ask_multiple(
                {"input_values": [1, 2, 3, 4], "raise_errors": False, "max_retries": 2},
                {"input_values": [5, 6, 7, 8], "raise_errors": False, "max_retries": 2},
                {"input_values": [9, 10, 11, 12], "raise_errors": False, "max_retries": 2},
                {"input_values": [13, 14, 15, 16], "raise_errors": False, "max_retries": 2},
            )

        # Check that all four questions succeeded.
        self.assertEqual(
            [answer[0] for answer in answers],
            [
                {"output_manifest": None, "output_values": [1, 2, 3, 4]},
                {"output_manifest": None, "output_values": [5, 6, 7, 8]},
                {"output_manifest": None, "output_values": [9, 10, 11, 12]},
                {"output_manifest": None, "output_values": [13, 14, 15, 16]},
            ],
        )
