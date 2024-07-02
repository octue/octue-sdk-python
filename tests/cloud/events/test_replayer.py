import json
import logging
import os
import unittest

from octue.cloud.events.replayer import EventReplayer
from tests import TEST_BUCKET_NAME, TESTS_DIR


class TestEventReplayer(unittest.TestCase):
    def test_with_no_events(self):
        """Test that `None` is returned if no events are passed in."""
        with self.assertLogs(level=logging.DEBUG) as logging_context:
            result = EventReplayer().handle_events(events=[])

        self.assertIsNone(result)
        self.assertIn("No events (or no valid events) were received.", logging_context.output[0])

    def test_with_no_valid_events(self):
        """Test that `None` is returned if no valid events are received."""
        with self.assertLogs(level=logging.DEBUG) as logging_context:
            result = EventReplayer().handle_events(events=[{"invalid": "event"}])

        self.assertIsNone(result)
        self.assertIn("received an event that doesn't conform", logging_context.output[1])
        self.assertIn("No events (or no valid events) were received.", logging_context.output[2])

    def test_no_result_event(self):
        """Test that `None` is returned if no result event is received."""
        event = {
            "event": {"kind": "delivery_acknowledgement"},
            "attributes": {
                "datetime": "2024-04-11T10:46:48.236064",
                "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
                "order": "0",
                "retry_count": 0,
                "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
                "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
                "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
                "parent": "octue/test-service:1.0.0",
                "originator": "octue/test-service:1.0.0",
                "sender": "octue/test-service:1.0.0",
                "sender_type": "CHILD",
                "sender_sdk_version": "0.51.0",
                "recipient": "octue/another-service:3.2.1",
            },
        }

        with self.assertLogs() as logging_context:
            result = EventReplayer().handle_events(events=[event])

        self.assertIsNone(result)
        self.assertIn("question was delivered", logging_context.output[0])

    def test_with_events_including_result_event(self):
        """Test that stored events can be replayed and the outputs extracted from the "result" event."""
        with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
            events = json.load(f)

        result = EventReplayer().handle_events(events)
        self.assertEqual(result["output_values"], [1, 2, 3, 4, 5])

        output_manifest = result["output_manifest"].to_primitive()
        del output_manifest["datasets"]["example_dataset"]["id"]

        self.assertEqual(
            output_manifest,
            {
                "id": "a13713ae-f207-41c6-9e29-0a848ced6039",
                "name": None,
                "datasets": {
                    "example_dataset": {
                        "name": "divergent-strange-gharial-of-pizza",
                        "tags": {},
                        "labels": [],
                        "path": "https://storage.googleapis.com/octue-sdk-python-test-bucket/example_output_datasets"
                        "/example_dataset/.signed_metadata_files/divergent-strange-gharial-of-pizza",
                        "files": [f"gs://{TEST_BUCKET_NAME}/example_output_datasets/example_dataset/output.dat"],
                    }
                },
            },
        )
