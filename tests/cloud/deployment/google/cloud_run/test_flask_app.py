import base64
import json
import uuid
from unittest import TestCase, mock

from octue.cloud.deployment.google.cloud_run import flask_app


flask_app.app.testing = True


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
        with flask_app.app.test_client() as client:
            with mock.patch("octue.cloud.deployment.google.cloud_run.flask_app.answer_question"):

                response = client.post(
                    "/",
                    json={
                        "deliveryAttempt": 1,
                        "subscription": "projects/my-project/subscriptions/my-subscription",
                        "message": {
                            "data": base64.b64encode(
                                json.dumps({"input_values": [1, 2, 3], "input_manifest": None}).encode()
                            ).decode(),
                            "attributes": {"question_uuid": str(uuid.uuid4()), "forward_logs": "1"},
                        },
                    },
                )

                self.assertEqual(response.status_code, 204)

    def test_redelivered_questions_are_acknowledged_and_ignored(self):
        """Test that redelivered questions are acknowledged and then ignored."""
        with flask_app.app.test_client() as client:
            with mock.patch(
                "octue.cloud.deployment.google.cloud_run.flask_app.answer_question"
            ) as mock_answer_question:

                response = client.post(
                    "/",
                    json={
                        "deliveryAttempt": 2,
                        "subscription": "projects/my-project/subscriptions/my-subscription",
                        "message": {
                            "data": {},
                            "attributes": {"question_uuid": str(uuid.uuid4()), "forward_logs": "1"},
                        },
                    },
                )

        self.assertEqual(response.status_code, 204)
        mock_answer_question.assert_not_called()
