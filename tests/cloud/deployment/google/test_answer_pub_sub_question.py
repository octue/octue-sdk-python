import json
import unittest.mock
from unittest import TestCase, mock

import yaml

from octue.cloud.deployment.google.answer_pub_sub_question import answer_question
from tests.cloud.pub_sub.mocks import MockTopic
from tests.mocks import MockOpen


class TestAnswerPubSubQuestion(TestCase):
    def test_with_no_app_configuration_file(self):
        """Test that the `answer_question` function uses the default service and app configuration values when the
        minimal service configuration is provided with no path to an app configuration file.
        """
        with mock.patch(
            "octue.configuration.open",
            unittest.mock.mock_open(read_data=yaml.dump({"services": [{"name": "test-service"}]})),
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
                "children": None,
                "output_location": None,
                "crash_analytics_cloud_path": None,
                "project_name": "a-project-name",
                "service_id": "test-service",
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
                        "services": [
                            {
                                "name": "test-service",
                                "app_source_path": "/path/to/app_dir",
                                "twine_path": "path/to/twine.json",
                                "app_configuration_path": "app_configuration.json",
                            }
                        ]
                    }
                ),
                "app_configuration.json": json.dumps({"configuration_values": {"hello": "configuration"}}),
            }

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
                "children": None,
                "output_location": None,
                "crash_analytics_cloud_path": None,
                "project_name": "a-project-name",
                "service_id": "test-service",
            }
        )
