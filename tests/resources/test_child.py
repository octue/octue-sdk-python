import functools
import os
import random
import time
from multiprocessing import Value
from unittest.mock import patch

from google.auth.exceptions import DefaultCredentialsError

from octue.cloud.emulators._pub_sub import MockAnalysis, MockService, MockSubscriber, MockSubscription, MockTopic
from octue.cloud.emulators.child import ServicePatcher
from octue.resources.child import Child
from octue.resources.service_backends import GCPPubSubBackend
from tests import MOCK_SERVICE_REVISION_TAG
from tests.base import BaseTestCase


class TestChild(BaseTestCase):
    def test_representation(self):
        """Test that children are represented correctly as a string."""
        self.assertEqual(
            repr(
                Child(
                    id=f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}",
                    backend={"name": "GCPPubSubBackend", "project_name": "blah"},
                )
            ),
            f"<Child('octue/my-child:{MOCK_SERVICE_REVISION_TAG}')>",
        )

    def test_instantiating_child_without_credentials(self):
        """Test that a child can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Child(
                id=f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}",
                backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            )

    def test_child_cannot_be_asked_question_without_credentials(self):
        """Test that a child cannot be asked a question without Google Cloud credentials being available."""
        with patch.dict(os.environ, clear=True):
            with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                    with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                        with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):

                            child = Child(
                                id=f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}",
                                backend={"name": "GCPPubSubBackend", "project_name": "blah"},
                            )

                            with self.assertRaises(DefaultCredentialsError):
                                child.ask({"some": "input"})

    def test_child_can_be_asked_multiple_questions(self):
        """Test that a child can be asked multiple questions."""

        def mock_run_function(analysis_id, input_values, *args, **kwargs):
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(
            backend=GCPPubSubBackend(project_name="blah"),
            service_id=f"testing/wind-speed:{MOCK_SERVICE_REVISION_TAG}",
            run_function=mock_run_function,
        )

        with ServicePatcher():
            with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                responding_service.serve()

                child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_name": "blah"})

                # Make sure the child's underlying mock service knows how to access the mock responding service.
                child._service.children[responding_service.id] = responding_service
                self.assertEqual(child.ask([1, 2, 3, 4])["output_values"], [1, 2, 3, 4])
                self.assertEqual(child.ask([5, 6, 7, 8])["output_values"], [5, 6, 7, 8])

    def test_child_can_be_asked_questions_in_parallel(self):
        """Test that a child can be asked multiple questions in parallel and return the answers in the correct order."""

        def mock_run_function(analysis_id, input_values, *args, **kwargs):
            time.sleep(random.randint(0, 2))
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(
            backend=GCPPubSubBackend(project_name="blah"),
            service_id=f"testing/service-for-parallelised-questions:{MOCK_SERVICE_REVISION_TAG}",
            run_function=mock_run_function,
        )

        with ServicePatcher():
            with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                responding_service.serve()

                child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_name": "blah"})

                # Make sure the child's underlying mock service knows how to access the mock responding service.
                child._service.children[responding_service.id] = responding_service

                answers = child.ask_multiple(
                    {"input_values": [1, 2, 3, 4]},
                    {"input_values": [5, 6, 7, 8]},
                )

                self.assertEqual(
                    answers,
                    [
                        {"output_values": [1, 2, 3, 4], "output_manifest": None},
                        {"output_values": [5, 6, 7, 8], "output_manifest": None},
                    ],
                )

    def test_error_raised_when_using_ask_multiple_and_one_question_fails(self):
        """Test that an error is raised if any of the questions given to `Child.ask_multiple` fail."""

        def mock_run_function_that_sometimes_fails(analysis_id, input_values, *args, **kwargs):
            kwargs["runs"].value += 1

            if kwargs["runs"].value % 2 == 0:
                raise Exception("Deliberately raised for `Child.ask_multiple` test.")

            time.sleep(random.randint(0, 2))
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(
            backend=GCPPubSubBackend(project_name="blah"),
            service_id=f"testing/service-for-parallelised-questions-failure:{MOCK_SERVICE_REVISION_TAG}",
            run_function=functools.partial(mock_run_function_that_sometimes_fails, runs=Value("d", 0)),
        )

        with ServicePatcher():
            with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                responding_service.serve()

                child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_name": "blah"})

                # Make sure the child's underlying mock service knows how to access the mock responding service.
                child._service.children[responding_service.id] = responding_service

                with self.assertRaises(Exception):
                    child.ask_multiple(
                        {"input_values": [1, 2, 3, 4]},
                        {"input_values": [5, 6, 7, 8]},
                        {"input_values": [9, 10, 11, 12]},
                    )
