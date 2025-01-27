import copy
import logging
import os
from unittest import TestCase
from unittest.mock import patch
import uuid

from google.api_core.exceptions import NotFound

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


class TestInvalidPayloads(TestCase):
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


class TestQuestionRedelivery(TestCase):
    def test_warning_logged_if_no_event_store_provided(self):
        """Test that the question is allowed to proceed to analysis and a warning is logged if the event store cannot be
        checked because one hasn't been specified in the service configuration.
        """
        mock_configuration = copy.deepcopy(MOCK_CONFIGURATION)
        mock_configuration.event_store_table_id = None

        with flask_app.app.test_client() as client:
            with patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_pub_sub_question"
            ) as mock_answer_question:
                with patch("octue.configuration.ServiceConfiguration.from_file", return_value=mock_configuration):
                    with self.assertLogs(level=logging.WARNING) as logging_context:
                        response = client.post(
                            "/",
                            json={
                                "deliveryAttempt": 1,
                                "subscription": "projects/my-project/subscriptions/my-subscription",
                                "message": {
                                    "data": {},
                                    "attributes": {
                                        "question_uuid": str(uuid.uuid4()),
                                        "forward_logs": "1",
                                        "retry_count": "0",
                                    },
                                },
                            },
                        )

        self.assertTrue(
            logging_context.output[0].endswith(
                "Cannot check if question has been redelivered as the 'event_store_table_id' key hasn't been set in "
                "the service configuration (`octue.yaml` file)."
            )
        )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_called_once()

    def test_warning_logged_if_event_store_not_found(self):
        """Test that the question is allowed to proceed to analysis and a warning is logged if the event store cannot be
        found.
        """
        mock_configuration = copy.deepcopy(MOCK_CONFIGURATION)
        mock_configuration.event_store_table_id = "nonexistent.table"

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=mock_configuration),
                patch("octue.cloud.deployment.google.cloud_run.flask_app.get_events", side_effect=NotFound("blah")),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_pub_sub_question"
            ) as mock_answer_question:
                with multi_patcher:
                    with self.assertLogs(level=logging.WARNING) as logging_context:
                        response = client.post(
                            "/",
                            json={
                                "deliveryAttempt": 1,
                                "subscription": "projects/my-project/subscriptions/my-subscription",
                                "message": {
                                    "data": {},
                                    "attributes": {
                                        "question_uuid": str(uuid.uuid4()),
                                        "forward_logs": "1",
                                        "retry_count": "0",
                                    },
                                },
                            },
                        )

        self.assertTrue(
            logging_context.output[0].endswith(
                "Cannot check if question has been redelivered as no event store table was found with the ID "
                "'nonexistent.table'; check that the 'event_store_table_id' key in the service configuration "
                "(`octue.yaml` file) is correct."
            )
        )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_called_once()

    def test_new_question(self):
        """Test that a new question is checked against the event store and allowed to proceed to analysis."""
        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch("octue.cloud.deployment.google.cloud_run.flask_app.get_events", return_value=[]),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_pub_sub_question"
            ) as mock_answer_question:
                with multi_patcher:
                    with self.assertLogs() as logging_context:
                        response = client.post(
                            "/",
                            json={
                                "deliveryAttempt": 1,
                                "subscription": "projects/my-project/subscriptions/my-subscription",
                                "message": {
                                    "data": {},
                                    "attributes": {
                                        "question_uuid": str(uuid.uuid4()),
                                        "forward_logs": "1",
                                        "retry_count": "0",
                                    },
                                },
                            },
                        )

        self.assertTrue(logging_context.output[0].endswith("is a new question."))
        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_called_once()

    def test_redelivered_questions_are_acknowledged_and_dropped(self):
        """Test that questions undesirably redelivered by Pub/Sub are acknowledged and dropped."""
        question_uuid = "fcd7aad7-dbf0-47d2-8984-220d493df2c1"

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch(
                    "octue.cloud.deployment.google.cloud_run.flask_app.get_events",
                    return_value=[{"attributes": {"retry_count": "0"}}],
                ),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_pub_sub_question"
            ) as mock_answer_question:
                with self.assertLogs(level=logging.WARNING) as logging_context:
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
                                        "retry_count": "0",
                                    },
                                },
                            },
                        )

        self.assertIn(
            "has already been received by the service. It will now be acknowledged and dropped to prevent further "
            "redundant redelivery.",
            logging_context.output[0],
        )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_not_called()

    def test_retried_questions_are_allowed(self):
        """Test that questions explicitly retried by the SDK are allowed to proceed to analysis."""
        question_uuid = "fcd7aad7-dbf0-47d2-8984-220d493df2c1"

        multi_patcher = MultiPatcher(
            patches=[
                patch("octue.configuration.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION),
                patch(
                    "octue.cloud.deployment.google.cloud_run.flask_app.get_events",
                    return_value=[{"attributes": {"retry_count": "0"}}],
                ),
            ]
        )

        with flask_app.app.test_client() as client:
            with patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_pub_sub_question"
            ) as mock_answer_question:
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
                                    "retry_count": "1",
                                },
                            },
                        },
                    )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_called_once()
