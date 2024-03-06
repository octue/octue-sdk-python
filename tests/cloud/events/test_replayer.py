import json
import os
import unittest

from octue.cloud.events.replayer import EventReplayer
from tests import TEST_BUCKET_NAME, TESTS_DIR


class TestEventReplayer(unittest.TestCase):
    def test_replay_events(self):
        """Test that stored events can be replayed and the result extracted."""
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
