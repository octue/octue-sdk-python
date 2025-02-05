import json
import os
from unittest import TestCase, mock
from unittest.mock import patch

import yaml

from octue.cloud.emulators._pub_sub import MockTopic
from octue.cloud.pub_sub.answer_question import answer_pub_sub_question
from octue.utils.patches import MultiPatcher
from tests.mocks import MockOpen


class TestAnswerPubSubQuestion(TestCase):
    def test_with_no_app_configuration_file(self):
        """Test that the `answer_question` function uses the default service and app configuration values when the
        minimal service configuration is provided with no path to an app configuration file.
        """
        with MultiPatcher(
            patches=[
                patch(
                    "octue.configuration.open",
                    mock.mock_open(
                        read_data=yaml.dump({"services": [{"name": "test-service", "namespace": "testing"}]})
                    ),
                ),
                patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
                patch("octue.cloud.pub_sub.answer_question.Service"),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "blah"}),
            ]
        ):
            with patch("octue.cloud.pub_sub.answer_question.Runner.from_configuration") as mock_constructor:
                answer_pub_sub_question(
                    question={
                        "data": {},
                        "attributes": {
                            "question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "parent_question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "originator_question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "parent": "some/originator:service",
                            "originator": "some/originator:service",
                            "retry_count": 0,
                        },
                    },
                    project_name="a-project-name",
                )

        self.assertTrue(
            mock_constructor.call_args.kwargs["service_configuration"].app_source_path.endswith("octue-sdk-python")
        )
        self.assertTrue(mock_constructor.call_args.kwargs["service_configuration"].twine_path.endswith("twine.json"))
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].diagnostics_cloud_path)
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].service_registries)

        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].configuration_values)
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].configuration_manifest)
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].children)
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].output_location)

        self.assertEqual(mock_constructor.call_args.kwargs["project_name"], "a-project-name")
        self.assertEqual(mock_constructor.call_args.kwargs["service_id"], "testing/test-service:blah")

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
                                "namespace": "testing",
                                "app_source_path": "/path/to/app_dir",
                                "twine_path": "path/to/twine.json",
                                "app_configuration_path": "app_configuration.json",
                            }
                        ]
                    }
                ),
                "app_configuration.json": json.dumps({"configuration_values": {"hello": "configuration"}}),
            }

        with patch("octue.cloud.pub_sub.answer_question.Runner.from_configuration") as mock_constructor:
            with MultiPatcher(
                patches=[
                    patch("octue.configuration.open", mock.mock_open(mock=MockOpenForConfigurationFiles)),
                    patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
                    patch("octue.cloud.pub_sub.answer_question.Service"),
                    patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "blah"}),
                ]
            ):
                answer_pub_sub_question(
                    question={
                        "data": {},
                        "attributes": {
                            "question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "parent_question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "originator_question_uuid": "8c859f87-b594-4297-883f-cd1c7718ef29",
                            "parent": "some/originator:service",
                            "originator": "some/originator:service",
                            "retry_count": 0,
                        },
                    },
                    project_name="a-project-name",
                )

        self.assertTrue(
            mock_constructor.call_args.kwargs["service_configuration"].app_source_path.endswith("path/to/app_dir")
        )
        self.assertTrue(
            mock_constructor.call_args.kwargs["service_configuration"].twine_path.endswith("path/to/twine.json")
        )
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].diagnostics_cloud_path)
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].service_registries)

        self.assertEqual(
            mock_constructor.call_args.kwargs["app_configuration"].configuration_values, {"hello": "configuration"}
        )
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].configuration_manifest)
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].children)
        self.assertIsNone(mock_constructor.call_args.kwargs["app_configuration"].output_location)

        self.assertEqual(mock_constructor.call_args.kwargs["project_name"], "a-project-name")
        self.assertEqual(mock_constructor.call_args.kwargs["service_id"], "testing/test-service:blah")
