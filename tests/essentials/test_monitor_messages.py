from octue.essentials.monitor_messages import send_estimated_seconds_remaining, send_status_text
from octue.resources.analysis import Analysis
from tests.base import BaseTestCase


class TestMonitorMessages(BaseTestCase):
    analysis = Analysis(
        twine='{"monitor_message_schema": {"$ref": "https://refs.schema.octue.com/octue/essential-monitors/0.0.2.json"}}'
    )

    def test_send_status_text(self):
        """Test that the helper can be used to send a status text monitor message that conforms to the online schema."""
        send_status_text(self.analysis, "Blah", "test-service")

    def test_send_estimated_seconds_remaining(self):
        """Test that the helper can be used to send an ETA monitor message that conforms to the online schema."""
        send_estimated_seconds_remaining(self.analysis, 30, "test-service")
