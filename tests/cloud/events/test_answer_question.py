import os
from unittest import TestCase, mock
from unittest.mock import patch

import yaml

from octue.cloud.events.answer_question import answer_question
from octue.configuration import ServiceConfiguration
from octue.twined.cloud.emulators._pub_sub import MockTopic
from octue.utils.patches import MultiPatcher
from tests.mocks import MockOpen


class TestAnswerQuestion(TestCase):
    def test_answer_question(self):
        """Test that the `answer_question` function uses the values in the service configuration correctly."""

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
                                "configuration_values": {"hello": "configuration"},
                            }
                        ],
                    }
                ),
            }

        with patch("octue.cloud.events.answer_question.Runner.from_configuration") as mock_constructor:
            with MultiPatcher(
                patches=[
                    patch("octue.configuration.open", mock.mock_open(mock=MockOpenForConfigurationFiles)),
                    patch("octue.cloud.pub_sub.service.Topic", new=MockTopic),
                    patch("octue.cloud.events.answer_question.Service"),
                    patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "blah"}),
                ]
            ):
                service_config = ServiceConfiguration.from_file()

                answer_question(
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
                    project_id="a-project-id",
                    service_configuration=service_config,
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
            mock_constructor.call_args.kwargs["service_configuration"].configuration_values,
            {"hello": "configuration"},
        )

        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].configuration_manifest)
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].children)
        self.assertIsNone(mock_constructor.call_args.kwargs["service_configuration"].output_location)

        self.assertEqual(mock_constructor.call_args.kwargs["project_id"], "a-project-id")
        self.assertEqual(mock_constructor.call_args.kwargs["service_id"], "testing/test-service:blah")
