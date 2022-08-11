import os
from unittest.mock import patch

from google.auth.exceptions import DefaultCredentialsError

from octue.cloud.emulators._pub_sub import MockAnalysis, MockService, MockSubscriber, MockSubscription
from octue.cloud.emulators.child import ServicePatcher
from octue.resources.child import Child
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase


class TestChild(BaseTestCase):
    def test_instantiating_child_without_credentials(self):
        """Test that a child can be instantiated without Google Cloud credentials."""
        with patch.dict(os.environ, clear=True):
            Child(
                id="my-child",
                backend={"name": "GCPPubSubBackend", "project_name": "blah"},
            )

    def test_child_cannot_be_asked_question_without_credentials(self):
        """Test that a child cannot be asked a question without Google Cloud credentials being available."""
        with patch.dict(os.environ, clear=True):
            with patch("octue.cloud.pub_sub.service.Topic"):
                with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                    with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                        with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):

                            child = Child(
                                id="my-child",
                                backend={"name": "GCPPubSubBackend", "project_name": "blah"},
                            )

                            with self.assertRaises(DefaultCredentialsError):
                                child.ask({"some": "input"})

    def test_child_can_be_asked_multiple_questions(self):
        """Test that a child can be asked multiple questions."""

        def run_function(analysis_id, input_values, *args, **kwargs):
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(
            backend=GCPPubSubBackend(project_name="blah"),
            service_id="testing/wind-speed",
            run_function=run_function,
        )

        with ServicePatcher():
            with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                responding_service.serve()

                child = Child(id=responding_service.id, backend={"name": "GCPPubSubBackend", "project_name": "blah"})

                # Make sure the child's underlying mock service knows how to access the mock responding service.
                child._service.children[responding_service.id] = responding_service
                self.assertEqual(child.ask([1, 2, 3, 4])["output_values"], [1, 2, 3, 4])
                self.assertEqual(child.ask([5, 6, 7, 8])["output_values"], [5, 6, 7, 8])
