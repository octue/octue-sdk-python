import uuid
from unittest.mock import patch

from octue.resources.child import Child
from octue.resources.service_backends import GCPPubSubBackend
from tests.base import BaseTestCase
from tests.cloud.pub_sub.mocks import MockAnalysis, MockService, MockSubscription, MockTopic


class TestChild(BaseTestCase):
    def test_child_can_be_asked_multiple_questions(self):
        """Test that a child can be asked multiple questions."""

        def run_function(analysis_id, input_values, input_manifest, analysis_log_handler):
            return MockAnalysis(output_values=input_values)

        responding_service = MockService(
            backend=GCPPubSubBackend(project_name="blah"), service_id=str(uuid.uuid4()), run_function=run_function
        )

        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                    responding_service.serve()

                    child = Child(
                        name="wind_speed",
                        id=responding_service.id,
                        backend={"name": "GCPPubSubBackend", "project_name": "blah"},
                    )

                    # Tell the child's underlying MockService where to find the responding service.
                    child._service.children[responding_service.id] = responding_service

                    self.assertEqual(child.ask(input_values=[1, 2, 3, 4])["output_values"], [1, 2, 3, 4])
                    self.assertEqual(child.ask(input_values=[5, 6, 7, 8])["output_values"], [5, 6, 7, 8])
