import json
import logging
import os
import unittest
from unittest.mock import patch

from octue.cloud.events.replayer import EventReplayer
from tests import TEST_BUCKET_NAME, TESTS_DIR


with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
    EVENTS = json.load(f)


EXPECTED_OUTPUT_MANIFEST = {
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
}


class TestEventReplayer(unittest.TestCase):
    def test_with_no_events(self):
        """Test that `None` is returned if no events are passed in."""
        result = EventReplayer().handle_events(events=[])
        self.assertIsNone(result)

    def test_with_no_valid_events(self):
        """Test that `None` is returned if no valid events are received."""
        with self.assertLogs(level=logging.DEBUG) as logging_context:
            result = EventReplayer().handle_events(events=[{"invalid": "event"}])

        self.assertIsNone(result)
        self.assertIn("received an event that doesn't conform", logging_context.output[1])

    def test_no_result_event(self):
        """Test that `None` is returned if no result event is received."""
        event = {
            "event": {"kind": "delivery_acknowledgement"},
            "attributes": {
                "datetime": "2024-04-11T10:46:48.236064",
                "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
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
        with self.assertLogs() as logging_context:
            result = EventReplayer().handle_events(EVENTS)

        # Check that all events have been handled (they produce log messages).
        self.assertEqual(len(logging_context.output), 7)
        self.assertEqual(result["output_values"], [1, 2, 3, 4, 5])

        output_manifest = result["output_manifest"].to_primitive()
        del output_manifest["datasets"]["example_dataset"]["id"]
        self.assertEqual(output_manifest, EXPECTED_OUTPUT_MANIFEST)

    def test_with_only_handle_result(self):
        """Test that non-result events are skipped if `only_handle_result=True`."""
        with patch(
            "octue.cloud.events.handler.AbstractEventHandler._handle_delivery_acknowledgement"
        ) as mock_handle_delivery_acknowledgement:
            with self.assertLogs() as logging_context:
                result = EventReplayer(only_handle_result=True).handle_events(EVENTS)

        # If `only_handle_result` is respected, the delivery acknowledgement handler won't have been called.
        mock_handle_delivery_acknowledgement.assert_not_called()

        # Check that only the result event haas been handled.
        self.assertEqual(len(logging_context.output), 1)
        self.assertEqual(result["output_values"], [1, 2, 3, 4, 5])

        output_manifest = result["output_manifest"].to_primitive()
        del output_manifest["datasets"]["example_dataset"]["id"]
        self.assertEqual(output_manifest, EXPECTED_OUTPUT_MANIFEST)

    def test_without_service_metadata_in_logs(self):
        """Test that log messages are formatted to not include the service metadata if
        `include_service_metadata_in_logs=False`.
        """
        with self.assertLogs() as logging_context:
            EventReplayer(include_service_metadata_in_logs=False).handle_events(EVENTS)

        for log_message in logging_context.output:
            self.assertNotIn("[octue/test-service:1.0.0 | d45c7e99-d610-413b-8130-dd6eef46dda6]", log_message)

    def test_without_validating_events(self):
        """Test that event validation is skipped when `validate_events=False`."""
        with patch("octue.cloud.events.handler.is_event_valid") as mock_is_event_valid:
            result = EventReplayer(validate_events=False).handle_events(EVENTS)

        mock_is_event_valid.assert_not_called()
        self.assertEqual(result["output_values"], [1, 2, 3, 4, 5])

        output_manifest = result["output_manifest"].to_primitive()
        del output_manifest["datasets"]["example_dataset"]["id"]
        self.assertEqual(output_manifest, EXPECTED_OUTPUT_MANIFEST)
