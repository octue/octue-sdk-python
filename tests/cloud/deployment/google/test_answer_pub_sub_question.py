import json
import os
import unittest.mock
from unittest import TestCase, mock

import yaml

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from octue.exceptions import MissingServiceID
from tests.cloud.pub_sub.mocks import MockTopic
from tests.mocks import MockOpen


SERVICE_ID = "octue.services.14b124f3-8ca9-4baf-99f5-0b79179f04a6"


class TestAnswerPubSubQuestion(TestCase):
    def test_error_raised_when_no_service_id_environment_variable(self):
        """Test that a MissingServiceID error is raised if the SERVICE_ID environment variable is missing."""
        with self.assertRaises(MissingServiceID):
            answer_question(
                question={
                    "data": {},
                    "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                },
                project_name="a-project-name",
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
                )

    def test_with_no_app_configuration_file(self):
        """Test that the `answer_question` function uses the default service and app configuration values when the
        minimal service configuration is provided with no path to an app configuration file.
        """
        with mock.patch.dict(os.environ, {"SERVICE_ID": SERVICE_ID}):
            with mock.patch(
                "octue.configuration.open", unittest.mock.mock_open(read_data=yaml.dump({"name": "test-service"}))
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
                            )

        mock_runner.assert_called_with(
            **{
                "app_src": ".",
                "twine": "twine.json",
                "configuration_values": None,
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "project_name": "a-project-name",
            }
        )

    def test_with_service_configuration_file_and_app_configuration_file(self):
        """Test that the `answer_question` function uses the values in the service and app configuration files if they
        are provided.
        """

        class MockOpenForConfigurationFiles(MockOpen):
            path_to_contents_mapping = {
                "octue.yaml": yaml.dump(
                    {
                        "name": "test-service",
                        "app_source_path": "/path/to/app_dir",
                        "twine_path": "path/to/twine.json",
                        "app_configuration_path": "app_configuration.json",
                    }
                ),
                "app_configuration.json": json.dumps({"configuration_values": {"hello": "configuration"}}),
            }

        with mock.patch.dict(os.environ, {"SERVICE_ID": SERVICE_ID}):
            with mock.patch("octue.configuration.open", unittest.mock.mock_open(mock=MockOpenForConfigurationFiles)):
                with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Runner") as mock_runner:
                    with mock.patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
                        with mock.patch("octue.cloud.deployment.google.answer_pub_sub_question.Service"):
                            answer_question(
                                question={
                                    "data": {},
                                    "attributes": {"question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29"},
                                },
                                project_name="a-project-name",
                            )

        mock_runner.assert_called_with(
            **{
                "app_src": "/path/to/app_dir",
                "twine": "path/to/twine.json",
                "configuration_values": {"hello": "configuration"},
                "configuration_manifest": None,
                "output_manifest_path": None,
                "children": None,
                "project_name": "a-project-name",
            }
        )
