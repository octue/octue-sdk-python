import os
import tempfile
from unittest import TestCase, mock

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.exceptions import MissingServiceID
from tests.cloud.pub_sub.mocks import MockTopic


SERVICE_ID = "octue.services.14b124f3-8ca9-4baf-99f5-0b79179f04a6"


class TestAnswerQuestion(TestCase):
    def test_error_raised_when_no_service_id_environment_variable(self):
        """Test that a MissingServiceID error is raised if the SERVICE_ID environment variable is missing."""
        with self.assertRaises(MissingServiceID):
            answer_question(
                question={
                    "data": {},
                    "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                },
                project_name="a-project-name",
                credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
            )

    def test_error_raised_when_service_id_environment_variable_is_empty(self):
        """Test that a MissingServiceID error is raised if the SERVICE_ID environment variable is empty."""
        with mock.patch.dict(os.environ, {"SERVICE_ID": ""}):
            with self.assertRaises(MissingServiceID):
                answer_question(
                    question={
                        "data": {},
                        "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                    },
                    project_name="a-project-name",
                    credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
                )

    def test_with_no_deployment_configuration_file(self):
        """Test that the Cloud Run `answer_question` function uses the default deployment values when a deployment
        configuration file is not provided.
        """
        with mock.patch.dict(os.environ, {"SERVICE_ID": SERVICE_ID}):
            with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Runner") as mock_runner:
                with mock.patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                    with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Service"):
                        answer_question(
                            question={
                                "data": {},
                                "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                            },
                            project_name="a-project-name",
                            credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
                        )

        mock_runner.assert_called_with(
            **{
                "app_src": ".",
                "twine": "twine.json",
                "configuration_values": None,
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "skip_checks": False,
                "project_name": "a-project-name",
            }
        )

    def test_with_deployment_configuration_file(self):
        """Test that the Cloud Run `answer_question` function uses the values in the deployment configuration file if it
        is provided.
        """
        with mock.patch.dict(
            os.environ,
            {"DEPLOYMENT_CONFIGURATION_PATH": tempfile.NamedTemporaryFile().name, "SERVICE_ID": SERVICE_ID},
        ):
            with mock.patch(
                "octue.cloud.deployment.google.answer_pub_sub_question._get_deployment_configuration",
                return_value={"app_dir": "/path/to/app_dir"},
            ):
                with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Runner") as mock_runner:
                    with mock.patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                        with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Service"):
                            answer_question(
                                question={
                                    "data": {},
                                    "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                                },
                                project_name="a-project-name",
                                credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS",
                            )

        mock_runner.assert_called_with(
            **{
                "app_src": "/path/to/app_dir",
                "twine": "twine.json",
                "configuration_values": None,
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "skip_checks": False,
                "project_name": "a-project-name",
            }
        )
