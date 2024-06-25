import base64
import json
import os
import uuid
from unittest import TestCase
from unittest.mock import patch

from octue.cloud.deployment.google.cloud_run import flask_app
from octue.configuration import ServiceConfiguration
from octue.utils.patches import MultiPatcher
from tests import TESTS_DIR


flask_app.app.testing = True


TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")

MOCK_CONFIGURATION = ServiceConfiguration(
    namespace="testing",
    name="test-app",
    app_source_path=os.path.join(TESTS_DIR, "test_app_modules", "app_module"),
    twine_path=TWINE_FILE_PATH,
    app_configuration_path="blah.json",
    event_store_table_id="mock-event-store-table-id",
)


class TestFlaskApp(TestCase):
    def test_post_to_index_with_no_payload_results_in_400_error(self):
        """Test that a 400 (bad request) error code is returned if no payload is sent to the Flask endpoint."""
        with flask_app.app.test_client() as client:
            response = client.post("/", json={"deliveryAttempt": 1})
            self.assertEqual(response.status_code, 400)

    def test_post_to_index_with_invalid_payload_results_in_400_error(self):
        """Test that a 400 (bad request) error code is returned if an invalid payload is sent to the Flask endpoint."""
        with flask_app.app.test_client() as client:
            response = client.post("/", json={"some": "data", "deliveryAttempt": 1})
            self.assertEqual(response.status_code, 400)

            response = client.post("/", json={"message": "data", "deliveryAttempt": 1})
            self.assertEqual(response.status_code, 400)

    def test_post_to_index_with_valid_payload(self):
        """Test that the Flask endpoint returns a 204 (ok, no content) response to a valid payload."""
        event = base64.b64encode(json.dumps({"input_values": [1, 2, 3], "input_manifest": None}).encode()).decode()
        attributes = {"question_uuid": str(uuid.uuid4()), "forward_logs": "1", "retry_count": 0}

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.cloud.deployment.google.cloud_run.flask_app.answer_question"),
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch("octue.cloud.deployment.google.cloud_run.flask_app.get_events", return_value=[]),
            ]
        )

        with flask_app.app.test_client() as client:
            with multi_patcher:
                response = client.post(
                    "/",
                    json={
                        "deliveryAttempt": 1,
                        "subscription": "projects/my-project/subscriptions/my-subscription",
                        "message": {"data": event, "attributes": attributes},
                    },
                )

        self.assertEqual(response.status_code, 204)

    def test_redelivered_questions_are_acknowledged_and_ignored(self):
        """Test that redelivered questions are acknowledged and then ignored."""
        question_uuid = "fcd7aad7-dbf0-47d2-8984-220d493df2c1"

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch(
                    "octue.cloud.deployment.google.cloud_run.flask_app.get_events",
                    return_value=[{"attributes": {"other_attributes": {"retry_count": 0}}}],
                ),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch("octue.cloud.deployment.google.cloud_run.flask_app.answer_question") as mock_answer_question:
                with multi_patcher:
                    response = client.post(
                        "/",
                        json={
                            "subscription": "projects/my-project/subscriptions/my-subscription",
                            "message": {
                                "data": {},
                                "attributes": {
                                    "question_uuid": question_uuid,
                                    "forward_logs": "1",
                                    "retry_count": 0,
                                },
                            },
                        },
                    )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_not_called()

    def test_retried_questions_are_allowed(self):
        """Test that retried questions with the same question UUID are allowed to proceed to analysis."""
        question_uuid = "fcd7aad7-dbf0-47d2-8984-220d493df2c1"

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch(
                    "octue.cloud.deployment.google.cloud_run.flask_app.get_events",
                    return_value=[{"attributes": {"other_attributes": {"retry_count": 0}}}],
                ),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch("octue.cloud.deployment.google.cloud_run.flask_app.answer_question") as mock_answer_question:
                with multi_patcher:
                    response = client.post(
                        "/",
                        json={
                            "subscription": "projects/my-project/subscriptions/my-subscription",
                            "message": {
                                "data": {},
                                "attributes": {
                                    "question_uuid": question_uuid,
                                    "forward_logs": "1",
                                    "retry_count": 1,
                                },
                            },
                        },
                    )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_called_once()
