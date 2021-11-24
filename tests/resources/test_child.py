import uuid
from unittest.mock import patch

from octue.resources.child import Child
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import MockAnalysis, MockService, MockSubscriber, MockSubscription, MockTopic


class TestChild(BaseTestCase):
    def test_child_can_be_asked_multiple_questions(self):
        """Test that a child can be asked multiple questions."""
        backend = GCPPubSubBackend(project_name="blah")

        def run_function(analysis_id, input_values, input_manifest, analysis_log_handler, handle_monitor_message):
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(backend=backend, service_id=str(uuid.uuid4()), run_function=run_function)

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                    with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                        responding_service.serve()

                        child = Child(
                            name="wind_speed",
                            id=responding_service.id,
                            backend={"name": "GCPPubSubBackend", "project_name": "blah"},
                        )

                        # Make sure the child's underlying mock service knows how to access the mock responding service.
                        child._service.children[responding_service.id] = responding_service
                        self.assertEqual(child.ask([1, 2, 3, 4])["output_values"], [1, 2, 3, 4])
                        self.assertEqual(child.ask([5, 6, 7, 8])["output_values"], [5, 6, 7, 8])
