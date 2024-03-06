from unittest import TestCase
from unittest.mock import patch

from octue.cloud.pub_sub.bigquery import get_events


class TestBigQuery(TestCase):
    def test_error_raised_if_event_kind_invalid(self):
        """Test that an error is raised if the event kind is invalid."""
        with self.assertRaises(ValueError):
            get_events(table_id="blah", question_uuid="blah", kind="frisbee_tournament")

    def test_without_kind(self):
        """Test the query used to retrieve events of all kinds."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT data FROM `blah`\nWHERE CONTAINS_SUBSTR(subscription_name, @question_uuid)\nORDER BY publish_time\n"
            "LIMIT @limit",
        )

    def test_with_kind(self):
        """Test the query used to retrieve events of a specific kind."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", kind="result")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT data FROM `blah`\nWHERE CONTAINS_SUBSTR(subscription_name, @question_uuid)\n"
            'AND JSON_EXTRACT_SCALAR(data, "$.kind") = "result"\nORDER BY publish_time\nLIMIT @limit',
        )

    def test_with_attributes(self):
        """Test the query used to retrieve attributes in addition to events."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", include_attributes=True)

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT data, attributes FROM `blah`\nWHERE CONTAINS_SUBSTR(subscription_name, @question_uuid)\n"
            "ORDER BY publish_time\nLIMIT @limit",
        )

    def test_with_pub_sub_metadata(self):
        """Test the query used to retrieve Pub/Sub metadata in addition to events."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", include_pub_sub_metadata=True)

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT data, subscription_name, message_id, publish_time FROM `blah`\n"
            "WHERE CONTAINS_SUBSTR(subscription_name, @question_uuid)\nORDER BY publish_time\nLIMIT @limit",
        )
