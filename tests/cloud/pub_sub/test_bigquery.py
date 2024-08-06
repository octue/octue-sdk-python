from unittest import TestCase
from unittest.mock import MagicMock, patch

from octue.cloud.pub_sub.bigquery import get_events


class MockEmptyResult:
    """A mock empty query result."""

    def result(self):
        return MagicMock(total_rows=0)


class MockEmptyBigQueryClient:
    """A mock BigQuery client that returns a mock empty query result."""

    def query(self, *args, **kwargs):
        return MockEmptyResult()


class TestGetEvents(TestCase):
    def test_error_raised_if_no_question_uuid_type_provided(self):
        """Test that an error is raised if none of `question_uuid`, `parent_question_uuid`, and
        `originator_question_uuid` are provided.
        """
        with self.assertRaises(ValueError):
            get_events(table_id="blah")

    def test_error_if_more_than_one_question_uuid_type_provided(self):
        """Test that an error is raised if more that one of `question_uuid`, `parent_question_uuid`, and
        `originator_question_uuid` are provided.
        """
        for kwargs in (
            {"question_uuid": "a", "parent_question_uuid": "b"},
            {"question_uuid": "a", "originator_question_uuid": "b"},
            {"parent_question_uuid": "a", "originator_question_uuid": "b"},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    get_events(table_id="blah", **kwargs)

    def test_error_raised_if_kinds_invalid(self):
        """Test that an error is raised if the event kinds are invalid."""
        for invalid_kinds in ["frisbee_tournament", ["frisbee_tournament"]]:
            with self.subTest(invalid_kinds=invalid_kinds):
                with self.assertRaises(ValueError):
                    get_events(table_id="blah", question_uuid="blah", kinds=invalid_kinds)

    def test_no_events_found(self):
        """Test that an empty list is returned if no events are found for the question UUID."""
        with patch("octue.cloud.pub_sub.bigquery.Client", MockEmptyBigQueryClient):
            events = get_events(table_id="blah", question_uuid="blah")

        self.assertEqual(events, [])

    def test_without_tail(self):
        """Test the non-tail query."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", tail=False)

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\n"
            "ORDER BY `datetime` ASC\n"
            "LIMIT @limit",
        )

    def test_without_kinds(self):
        """Test the query used to retrieve events of all kinds."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )

    def test_with_kind(self):
        """Test the query used to retrieve events of a specific kind."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", kinds=["result"])

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\nAND kind IN ('result')\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )

    def test_with_kinds(self):
        """Test the query used to retrieve events of pecific kinds."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", kinds=["result", "question"])

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\nAND kind IN "
            "('result', 'question')\nORDER BY `datetime` DESC\nLIMIT @limit\n) ORDER BY `datetime` ASC",
        )

    def test_with_exclude_kind(self):
        """Test the query used to retrieve events of all kinds except a specific kind."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", exclude_kinds=["result"])

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\nAND kind NOT IN ('result')\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )

    def test_with_exclude_kinds(self):
        """Test the query used to retrieve events of all kinds except specific kinds."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", exclude_kinds=["result", "question"])

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE question_uuid=@relevant_question_uuid\nAND kind "
            "NOT IN ('result', 'question')\nORDER BY `datetime` DESC\nLIMIT @limit\n) ORDER BY `datetime` ASC",
        )

    def test_with_backend_metadata(self):
        """Test the query used to retrieve backend metadata in addition to events."""
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", question_uuid="blah", include_backend_metadata=True)

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes`, `backend`, `backend_metadata` FROM `blah`\n"
            "WHERE question_uuid=@relevant_question_uuid\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )

    def test_with_parent_question_uuid(self):
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", parent_question_uuid="blah")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE parent_question_uuid=@relevant_question_uuid\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )

    def test_with_originator_parent_question_uuid(self):
        with patch("octue.cloud.pub_sub.bigquery.Client") as mock_client:
            get_events(table_id="blah", originator_question_uuid="blah")

        self.assertEqual(
            mock_client.mock_calls[1].args[0],
            "SELECT * FROM (\n"
            "SELECT `originator_question_uuid`, `parent_question_uuid`, `question_uuid`, `kind`, `event`, `datetime`, "
            "`uuid`, `originator`, `parent`, `sender`, `sender_type`, `sender_sdk_version`, `recipient`, "
            "`other_attributes` FROM `blah`\nWHERE originator_question_uuid=@relevant_question_uuid\n"
            "ORDER BY `datetime` DESC\n"
            "LIMIT @limit\n"
            ") ORDER BY `datetime` ASC",
        )
