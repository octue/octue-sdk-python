from unittest.mock import patch

from octue.essentials.monitor_messages import send_estimated_seconds_remaining, send_status_text
from octue.resources.analysis import Analysis
from tests.base import BaseTestCase


# Version 0.0.2 of the essential monitors schema (reproduced here to avoid reliance on the availability of cloud
# storage buckets).
ESSENTIAL_MONITORS_SCHEMA = {
    "title": "Essential monitoring messages",
    "description": "A generalised schema allowing a limited but useful range of monitor messages from octue services.",
    "type": "object",
    "oneOf": [
        {
            "title": "Status text",
            "description": "A short update on progress or status of the service",
            "type": "object",
            "properties": {
                "status_text": {"type": "string"},
                "date_time": {
                    "description": "The date and time of the message in ISO format",
                    "type": "string",
                    "format": "date-time",
                },
            },
            "required": ["status_text", "date_time"],
        },
        {
            "title": "Estimated seconds remaining",
            "description": "An estimate of the number of seconds yet to elapse until the question is answered, useful for long running tasks.",
            "type": "object",
            "properties": {
                "estimated_seconds_remaining": {"type": "number", "exclusiveMinimum": 0},
                "date_time": {
                    "description": "The date and time of the message in ISO format",
                    "type": "string",
                    "format": "date-time",
                },
            },
            "required": ["estimated_seconds_remaining", "date_time"],
        },
        {
            "title": "Chart",
            "description": "A plotly-compliant chart object",
            "type": "object",
            "properties": {
                "chart": {"$ref": "https://api.plot.ly/v2/plot-schema?format=json&sha1=%27%27"},
                "date_time": {
                    "description": "The date and time of the message in ISO format",
                    "type": "string",
                    "format": "date-time",
                },
            },
            "required": ["chart", "date_time"],
        },
    ],
}


class TestMonitorMessages(BaseTestCase):
    analysis = Analysis(
        twine='{"monitor_message_schema": {"$ref": "https://refs.schema.octue.com/octue/essential-monitors/0.0.2.json"}}'
    )

    def test_send_status_text(self):
        """Test that the helper can be used to send a status text monitor message that conforms to the online schema."""
        with patch("twined.twine.Twine._get_schema", return_value=ESSENTIAL_MONITORS_SCHEMA):
            send_status_text(self.analysis, "Blah", "test-service")

    def test_send_estimated_seconds_remaining(self):
        """Test that the helper can be used to send an ETA monitor message that conforms to the online schema."""
        with patch("twined.twine.Twine._get_schema", return_value=ESSENTIAL_MONITORS_SCHEMA):
            send_estimated_seconds_remaining(self.analysis, 30, "test-service")
