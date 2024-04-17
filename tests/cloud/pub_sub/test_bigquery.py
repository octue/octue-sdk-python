from unittest import TestCase
from unittest.mock import patch

from octue.cloud.pub_sub.bigquery import get_events


class TestGetEvents(TestCase):
    def test_error_raised_if_event_kind_invalid(self):
        """Test that an error is raised if the event kind is invalid."""
        with self.assertRaises(ValueError):
            get_events(
                table_id="blah",
                sender="octue/test-service:1.0.0",
                question_uuid="blah",
                kind="frisbee_tournament",
            )

    def test_without_kind(self):
        """Test the query used to retrieve events of all kinds."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", sender="octue/test-service:1.0.0", question_uuid="blah")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT `event`, `kind`, `datetime`, `uuid`, `originator`, `sender`, `sender_type`, `sender_sdk_version`, "
            "`recipient`, `order`, `other_attributes` FROM `blah`\nWHERE sender=@sender\n"
            "AND question_uuid=@question_uuid\nORDER BY `order`\nLIMIT @limit",
        )

    def test_with_kind(self):
        """Test the query used to retrieve events of a specific kind."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", sender="octue/test-service:1.0.0", question_uuid="blah", kind="result")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT `event`, `kind`, `datetime`, `uuid`, `originator`, `sender`, `sender_type`, `sender_sdk_version`, "
            "`recipient`, `order`, `other_attributes` FROM `blah`\nWHERE sender=@sender\n"
            "AND question_uuid=@question_uuid\nAND kind='result'\nORDER BY `order`\nLIMIT @limit",
        )

    def test_with_backend_metadata(self):
        """Test the query used to retrieve backend metadata in addition to events."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(
                table_id="blah",
                sender="octue/test-service:1.0.0",
                question_uuid="blah",
                include_backend_metadata=True,
            )

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT `event`, `kind`, `datetime`, `uuid`, `originator`, `sender`, `sender_type`, `sender_sdk_version`, "
            "`recipient`, `order`, `other_attributes`, `backend`, `backend_metadata` FROM `blah`\n"
            "WHERE sender=@sender\nAND question_uuid=@question_uuid\nORDER BY `order`\nLIMIT @limit",
        )
