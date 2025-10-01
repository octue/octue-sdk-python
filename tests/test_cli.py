import json
import logging
import os
import tempfile
from unittest import mock
from unittest.mock import patch

from click.testing import CliRunner

from octue.cli import octue_cli
from octue.cloud import storage
from octue.resources import Dataset, Manifest
from octue.twined.cloud.emulators._pub_sub import MockService
from octue.twined.cloud.emulators.service import ServicePatcher
from octue.twined.configuration import ServiceConfiguration
from octue.utils.patches import MultiPatcher
from tests import MOCK_SERVICE_REVISION_TAG, TEST_BUCKET_NAME, TESTS_DIR
from tests.base import BaseTestCase

TWINE_FILE_PATH = os.path.join(TESTS_DIR, "data", "twines", "valid_schema_twine.json")

MOCK_CONFIGURATION = ServiceConfiguration(
    name="test-app",
    namespace="testing",
    configuration_values={"n_iterations": 5},
)

RESULT = {"output_values": {"some": "data"}}


class TestCLI(BaseTestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(octue_cli, ["--version"])
        self.assertIn("version", result.output)

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(octue_cli, ["--help"])
        self.assertTrue(help_result.output.startswith("Usage"))

        h_result = CliRunner().invoke(octue_cli, ["-h"])
        self.assertEqual(help_result.output, h_result.output)


class TestQuestionAskRemoteCommand(BaseTestCase):
    SRUID = "my-org/my-service:1.0.0"
    QUESTION_UUID = "81f35b28-068b-4314-9eeb-e55e60d0fe8a"

    def test_with_input_values(self):
        """Test that the `octue twined question ask remote` CLI command works with just input values."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.Child.ask", return_value=(RESULT, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask",
                        self.SRUID,
                        '--input-values={"height": 3}',
                    ],
                )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=False)
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_input_manifest(self):
        """Test that the `octue twined question ask remote` CLI command works with just an input manifest."""
        input_manifest = self.create_valid_manifest()

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.Child.ask", return_value=(RESULT, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask",
                        self.SRUID,
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        self.assertEqual(mock_ask.call_args.kwargs["input_manifest"].id, input_manifest.id)
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_input_values_and_manifest(self):
        """Test that the `octue twined question ask remote` CLI command works with input values and input manifest."""
        input_values = {"height": 3}
        input_manifest = self.create_valid_manifest()

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.Child.ask", return_value=(RESULT, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask",
                        self.SRUID,
                        f"--input-values={json.dumps(input_values)}",
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        self.assertEqual(mock_ask.call_args.kwargs["input_values"], input_values)
        self.assertEqual(mock_ask.call_args.kwargs["input_manifest"].id, input_manifest.id)
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_output_manifest(self):
        """Test that the `octue twined question ask remote` CLI command returns output manifests in a useful form."""
        result = {"output_values": {"some": "data"}, "output_manifest": self.create_valid_manifest()}

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.Child.ask", return_value=(result, self.QUESTION_UUID)):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask",
                        self.SRUID,
                        f"--input-values={json.dumps({'height': 3})}",
                    ],
                )

        output = json.loads(result.output)
        self.assertEqual(output["output_values"], {"some": "data"})
        self.assertEqual(len(output["output_manifest"]["datasets"]), 2)

    def test_asynchronous(self):
        """Test that the `octue twined question ask remote` CLI command works with the `--asynchronous` option and returns the
        question UUID.
        """
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.Child.ask", return_value=(RESULT, self.QUESTION_UUID)) as mock_ask:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask",
                        self.SRUID,
                        '--input-values={"height": 3}',
                        "--asynchronous",
                    ],
                )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=True)
        self.assertIn(self.QUESTION_UUID, result.output)

    def test_with_no_service_configuration(self):
        """Test that the command works when no service configuration is provided."""
        with mock.patch("octue.cli.Child.ask", return_value=(RESULT, self.QUESTION_UUID)) as mock_ask:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "twined",
                    "question",
                    "ask",
                    self.SRUID,
                    '--input-values={"height": 3}',
                ],
            )

        mock_ask.assert_called_with(input_values={"height": 3}, input_manifest=None, asynchronous=False)
        self.assertIn(json.dumps(RESULT), result.output)


class TestQuestionAskLocalCommand(BaseTestCase):
    def test_with_input_values(self):
        """Test that the `octue twined question ask local` CLI command works with just input values and sends an originator
        question.
        """
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.answer_question", return_value=RESULT) as mock_answer_question:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask-local",
                        '--input-values={"height": 3}',
                    ],
                )

        # Check service configuration.
        mock_answer_question_kwargs = mock_answer_question.call_args.kwargs
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].namespace, "testing")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].name, "test-app")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].configuration_values, {"n_iterations": 5})

        # Check question event.
        question = mock_answer_question_kwargs["question"]
        self.assertEqual(question["event"], {"kind": "question", "input_values": {"height": 3}})

        # Check question attributes.
        self.assertTrue(question["attributes"]["recipient"].startswith("testing/test-app"))
        # The question should be an originator question.
        self.assertEqual(question["attributes"]["question_uuid"], question["attributes"]["originator_question_uuid"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["originator"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["sender"])
        self.assertNotIn("parent_question_uuid", question["attributes"])

        # Check the result is in the output.
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_input_manifest(self):
        """Test that the `octue twined question ask local` CLI command works with just an input manifest and sends an
        originator question.
        """
        input_manifest = self.create_valid_manifest()
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.answer_question", return_value=RESULT) as mock_answer_question:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask-local",
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        # Check service configuration.
        mock_answer_question_kwargs = mock_answer_question.call_args.kwargs
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].namespace, "testing")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].name, "test-app")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].configuration_values, {"n_iterations": 5})

        # Check question event.
        question = mock_answer_question_kwargs["question"]
        self.assertEqual(Manifest.deserialise(question["event"]["input_manifest"]).id, input_manifest.id)

        # Check question attributes.
        self.assertTrue(question["attributes"]["recipient"].startswith("testing/test-app"))
        # The question should be an originator question.
        self.assertEqual(question["attributes"]["question_uuid"], question["attributes"]["originator_question_uuid"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["originator"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["sender"])
        self.assertNotIn("parent_question_uuid", question["attributes"])

        # Check the result is in the output.
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_input_values_and_manifest(self):
        """Test that the `octue twined question ask local` CLI command works with input values and input manifest and sends an
        originator question.
        """
        input_values = {"height": 3}
        input_manifest = self.create_valid_manifest()

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.answer_question", return_value=RESULT) as mock_answer_question:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask-local",
                        f"--input-values={json.dumps(input_values)}",
                        f"--input-manifest={input_manifest.serialise()}",
                    ],
                )

        # Check service configuration.
        mock_answer_question_kwargs = mock_answer_question.call_args.kwargs
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].namespace, "testing")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].name, "test-app")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].configuration_values, {"n_iterations": 5})

        # Check question event.
        question = mock_answer_question_kwargs["question"]
        self.assertEqual(question["event"]["input_values"], input_values)
        self.assertEqual(Manifest.deserialise(question["event"]["input_manifest"]).id, input_manifest.id)

        # Check question attributes.
        self.assertTrue(question["attributes"]["recipient"].startswith("testing/test-app"))
        # The question should be an originator question.
        self.assertEqual(question["attributes"]["question_uuid"], question["attributes"]["originator_question_uuid"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["originator"])
        self.assertEqual(question["attributes"]["parent"], question["attributes"]["sender"])
        self.assertNotIn("parent_question_uuid", question["attributes"])

        # Check the result is in the output.
        self.assertIn(json.dumps(RESULT), result.output)

    def test_with_output_manifest(self):
        """Test that the `octue twined question ask local` CLI command returns output manifests in a useful form."""
        result = {"output_values": {"some": "data"}, "output_manifest": self.create_valid_manifest()}

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.answer_question", return_value=result):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask-local",
                        f"--input-values={json.dumps({'height': 3})}",
                    ],
                )

        output = json.loads(result.output)
        self.assertEqual(output["output_values"], {"some": "data"})
        self.assertEqual(len(output["output_manifest"]["datasets"]), 2)

    def test_with_attributes(self):
        """Test that the `octue twined question ask remote` CLI command can be passed question attributes which are passed
        along to the answering `Service` instance.
        """
        original_attributes = {
            "datetime": "2024-04-11T10:46:48.236064",
            "uuid": "a9de11b1-e88f-43fa-b3a4-40a590c3443f",
            "question_uuid": "d45c7e99-d610-413b-8130-dd6eef46dda6",
            "parent_question_uuid": "5776ad74-52a6-46f7-a526-90421d91b8b2",
            "originator_question_uuid": "86dc55b2-4282-42bd-92d0-bd4991ae7356",
            "parent": "octue/the-parent:1.0.0",
            "originator": "octue/the-originator:1.0.5",
            "sender": "octue/the-sender:2.0.0",
            "sender_type": "CHILD",
            "sender_sdk_version": "0.51.0",
            "recipient": "octue/the-recipient:0.3.2",
            "retry_count": 1,
            "forward_logs": True,
            "save_diagnostics": "SAVE_DIAGNOSTICS_OFF",
            "cpus": 3,
            "memory": "10Gi",
            "ephemeral_storage": "500Mi",
        }

        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.answer_question", return_value=RESULT) as mock_answer_question:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "ask-local",
                        '--input-values={"height": 3}',
                        f"--attributes={json.dumps(original_attributes)}",
                    ],
                )

        # Check service configuration.
        mock_answer_question_kwargs = mock_answer_question.call_args.kwargs
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].namespace, "testing")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].name, "test-app")
        self.assertEqual(mock_answer_question_kwargs["service_configuration"].configuration_values, {"n_iterations": 5})

        # Check question event and attributes.
        question = mock_answer_question_kwargs["question"]
        self.assertEqual(question["event"], {"kind": "question", "input_values": {"height": 3}})
        self.assertEqual(question["attributes"], original_attributes)

        # Check the result is in the output.
        self.assertIn(json.dumps(RESULT), result.output)


class TestQuestionEventsGetCommand(BaseTestCase):
    QUESTION_UUID = "3ffc192c-7db0-4941-9b66-328a9fc02b62"

    with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
        EVENTS = json.load(f)

    def test_warning_logged_if_no_events_found(self):
        """Test that a warning is logged if no events are found for a question."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=[]):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "get",
                        "--question-uuid",
                        self.QUESTION_UUID,
                    ],
                )

        self.assertTrue(result.output.endswith("[]\n"))

    def test_with_question_uuid_types(self):
        """Test that each of the question UUID types is valid as an argument by themselves."""
        for question_uuid_arg in (
            "--question-uuid",
            "--parent-question-uuid",
            "--originator-question-uuid",
        ):
            with self.subTest(question_uuid_arg=question_uuid_arg):
                with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
                    with mock.patch("octue.cli.get_events", return_value=self.EVENTS):
                        result = CliRunner().invoke(
                            octue_cli,
                            [
                                "twined",
                                "question",
                                "events",
                                "get",
                                question_uuid_arg,
                                self.QUESTION_UUID,
                            ],
                        )

                self.assertEqual(json.loads(result.output), self.EVENTS)

    def test_with_kinds(self):
        """Test that the `--kinds` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[-2:-1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "get",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--kinds",
                        "result",
                    ],
                )

        self.assertEqual(mock_get_events.call_args.kwargs["kinds"], ["result"])
        self.assertIsNone(mock_get_events.call_args.kwargs["exclude_kinds"])
        self.assertEqual(json.loads(result.output), self.EVENTS[-2:-1])

    def test_with_exclude_kinds(self):
        """Test that the `--exclude-kinds` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[:1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "get",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--exclude-kinds",
                        "log_record,result,heartbeat",
                    ],
                )

        self.assertIsNone(mock_get_events.call_args.kwargs["kinds"])
        self.assertEqual(mock_get_events.call_args.kwargs["exclude_kinds"], ["log_record", "result", "heartbeat"])
        self.assertEqual(json.loads(result.output), self.EVENTS[:1])

    def test_with_limit(self):
        """Test that the `--limit` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[:1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "get",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--limit",
                        "1",
                    ],
                )

        self.assertEqual(mock_get_events.call_args.kwargs["limit"], 1)
        self.assertEqual(json.loads(result.output), self.EVENTS[:1])


class TestQuestionEventsReplayCommand(BaseTestCase):
    QUESTION_UUID = "3ffc192c-7db0-4941-9b66-328a9fc02b62"

    with open(os.path.join(TESTS_DIR, "data", "events.json")) as f:
        EVENTS = json.load(f)

    maxDiff = None

    def test_with_no_events_found(self):
        """Test that an empty list is passed to `stdout` if no events are found for a question."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=[]):
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "replay",
                        "--question-uuid",
                        self.QUESTION_UUID,
                    ],
                )

        self.assertEqual(result.output, "")

    def test_with_question_uuid_types_and_validate_events(self):
        """Test that each of the question UUID types is valid as an argument by themselves and that replaying still
        works with the `--validate-events` option.
        """
        for options in (
            ["--question-uuid"],
            ["--parent-question-uuid"],
            ["--originator-question-uuid"],
            ["--validate-events", "--question-uuid"],
        ):
            with self.subTest(question_uuid_arg=options):
                with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
                    with mock.patch("octue.cli.get_events", return_value=self.EVENTS):
                        with self.assertLogs() as logging_context:
                            result = CliRunner().invoke(
                                octue_cli,
                                [
                                    "twined",
                                    "question",
                                    "events",
                                    "replay",
                                    *options,
                                    self.QUESTION_UUID,
                                ],
                            )

                replayed_log_messages = "\n".join(logging_context.output)
                log_record_events = [event for event in self.EVENTS if event["event"]["kind"] == "log_record"]

                # Check logs messages were replayed.
                for event in log_record_events:
                    self.assertIn(event["event"]["log_record"]["msg"], replayed_log_messages)

                # Check result is correct.
                self.assertEqual(
                    json.loads(result.output.splitlines()[-1])["output_values"],
                    self.EVENTS[-2]["event"]["output_values"],
                )

    def test_with_kinds(self):
        """Test that the `--kinds` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[-2:-1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "replay",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--kinds",
                        "result",
                    ],
                )

        # Check result is correct.
        self.assertEqual(
            json.loads(result.output.splitlines()[-1])["output_values"],
            self.EVENTS[-2]["event"]["output_values"],
        )

        self.assertEqual(mock_get_events.call_args.kwargs["kinds"], ["result"])
        self.assertIsNone(mock_get_events.call_args.kwargs["exclude_kinds"])

    def test_with_exclude_kinds(self):
        """Test that the `--exclude-kinds` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[:1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "replay",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--exclude-kinds",
                        "log_record,result,heartbeat",
                    ],
                )

        # Check result isn't replayed.
        self.assertTrue(result.output.splitlines()[-1].endswith("No result was found for this question."))
        self.assertIsNone(mock_get_events.call_args.kwargs["kinds"])
        self.assertEqual(mock_get_events.call_args.kwargs["exclude_kinds"], ["log_record", "result", "heartbeat"])

    def test_with_limit(self):
        """Test that the `--limit` option is respected."""
        with mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=MOCK_CONFIGURATION):
            with mock.patch("octue.cli.get_events", return_value=self.EVENTS[:1]) as mock_get_events:
                result = CliRunner().invoke(
                    octue_cli,
                    [
                        "twined",
                        "question",
                        "events",
                        "replay",
                        "--question-uuid",
                        self.QUESTION_UUID,
                        "--limit",
                        "1",
                    ],
                )

        # Check result isn't replayed.
        self.assertTrue(result.output.splitlines()[-1].endswith("No result was found for this question."))
        self.assertEqual(mock_get_events.call_args.kwargs["limit"], 1)


class TestStartCommand(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.python_fractal_service_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "octue",
            "twined",
            "templates",
            "template-fractal",
        )

        cls.service_configuration = ServiceConfiguration(
            name="test-service",
            namespace="testing",
            app_source_path=cls.python_fractal_service_path,
            twine_path=os.path.join(cls.python_fractal_service_path, "twine.json"),
            configuration_values={
                "width": 600,
                "height": 600,
                "n_iterations": 64,
                "color_scale": "YlGnBu",
                "type": "png",
                "x_range": [-1.5, 0.6],
                "y_range": [-1.26, 1.26],
                "backend": {
                    "name": "GCPPubSubBackend",
                    "project_id": "octue-sdk-python",
                },
            },
        )

    def test_start_command(self):
        """Test that the start command works without error and uses the revision tag supplied in the
        `OCTUE_SERVICE_REVISION_TAG` environment variable.
        """
        with MultiPatcher(
            patches=[
                mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.service_configuration),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs(level=logging.INFO) as logging_context:
                    result = CliRunner().invoke(octue_cli, ["twined", "service", "start", "--timeout=0"])

        self.assertEqual(logging_context.records[1].message, "Starting <MockService('testing/test-service:goodbye')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_command_with_revision_tag_override_when_revision_tag_environment_variable_specified(self):
        """Test that the `OCTUE_SERVICE_REVISION_TAG` is overridden by the `--revision-tag` CLI option and that a
        warning is logged when this happens.
        """
        with MultiPatcher(
            patches=[
                mock.patch("octue.cli.ServiceConfiguration.from_file", return_value=self.service_configuration),
                mock.patch("octue.cli.Service", MockService),
                patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "goodbye"}),
            ]
        ):
            with ServicePatcher():
                with self.assertLogs() as logging_context:
                    result = CliRunner().invoke(
                        octue_cli,
                        ["twined", "service", "start", "--revision-tag=hello", "--timeout=0"],
                    )

        self.assertEqual(
            logging_context.records[1].message,
            "The `OCTUE_SERVICE_REVISION_TAG` environment variable 'goodbye' has been overridden by the "
            "`--revision-tag` CLI option 'hello'.",
        )

        self.assertEqual(logging_context.records[2].message, "Starting <MockService('testing/test-service:hello')>.")
        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)


class TestQuestionDiagnosticsCommand(BaseTestCase):
    DIAGNOSTICS_CLOUD_PATH = storage.path.generate_gs_path(TEST_BUCKET_NAME, "diagnostics")
    ANALYSIS_ID = "dc1f09ca-7037-484f-a394-8bd04866f924"

    @classmethod
    def setUpClass(cls):
        """Upload the test diagnostics data to the cloud storage emulator so the `octue twined question diagnostics` CLI command can
        be tested.

        :return None:
        """
        super().setUpClass()

        diagnostics = Dataset(
            path=os.path.join(TESTS_DIR, "data", "diagnostics"),
            recursive=True,
            include_octue_metadata_files=True,
        )

        diagnostics.upload(storage.path.join(cls.DIAGNOSTICS_CLOUD_PATH, cls.ANALYSIS_ID))

    def test_warning_logged_if_no_diagnostics_found(self):
        """Test that a warning about there being no diagnostics is logged if the diagnostics cloud path is empty."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "twined",
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, "9f4ccee3-15b0-4a03-b5ac-c19e1d66a709"),
                    "--local-path",
                    temporary_directory,
                ],
            )

        self.assertIn(
            "Attempted to download files from 'gs://octue-sdk-python-test-bucket/diagnostics/9f4ccee3-15b0-4a03-b5ac-"
            "c19e1d66a709' but it appears empty. Please check this is the correct path.",
            result.output,
        )

        self.assertIn(
            "No diagnostics found at 'gs://octue-sdk-python-test-bucket/diagnostics/9f4ccee3-15b0-4a03-b5ac-"
            "c19e1d66a709'",
            result.output,
        )

        self.assertNotIn("Downloaded diagnostics from", result.output)

    def test_get_diagnostics(self):
        """Test that only the values files, manifests, and questions file are downloaded when using the
        `question diagnostics` CLI command.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "twined",
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
                    "--local-path",
                    temporary_directory,
                ],
            )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            # Only the values files, manifests, and messages should be downloaded.
            directory_contents = list(os.walk(temporary_directory))
            self.assertEqual(len(directory_contents), 2)
            self.assertEqual(directory_contents[0][1], [self.ANALYSIS_ID])

            self.assertEqual(directory_contents[1][1], [])

            self.assertEqual(
                set(directory_contents[1][2]),
                {
                    "configuration_values.json",
                    "configuration_manifest.json",
                    "input_manifest.json",
                    "input_values.json",
                    "questions.json",
                },
            )

            # Check the questions have been downloaded.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "questions.json")) as f:
                questions = json.load(f)

            self.assertEqual(questions[0]["id"], f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}")

            self.assertEqual(
                questions[0]["events"],
                [
                    {"kind": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"kind": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"kind": "monitor_message", "data": {"sample": "data"}},
                    {"kind": "result", "output_values": [1, 2, 3, 4, 5]},
                ],
            )

    def test_get_diagnostics_with_datasets(self):
        """Test that datasets are downloaded as well as the values files, manifests, and questions file when the
        `question diagnostics` CLI command is run with the `--download-datasets` flag.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = CliRunner().invoke(
                octue_cli,
                [
                    "twined",
                    "question",
                    "diagnostics",
                    storage.path.join(self.DIAGNOSTICS_CLOUD_PATH, self.ANALYSIS_ID),
                    "--local-path",
                    temporary_directory,
                    "--download-datasets",
                ],
            )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            # Check the configuration dataset has been downloaded.
            configuration_dataset_path = os.path.join(
                temporary_directory,
                self.ANALYSIS_ID,
                "configuration_manifest_datasets",
                "configuration_dataset",
            )

            configuration_dataset = Dataset(configuration_dataset_path)
            self.assertEqual(configuration_dataset.tags, {"some": "metadata"})
            self.assertEqual(configuration_dataset.files.one().name, "my_file.txt")

            # Check that the configuration manifest has been updated to use the local paths for its datasets.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "configuration_manifest.json")) as f:
                self.assertEqual(json.load(f)["datasets"]["configuration_dataset"], configuration_dataset_path)

            # Check the input dataset has been downloaded.
            input_dataset_path = os.path.join(
                temporary_directory,
                self.ANALYSIS_ID,
                "input_manifest_datasets",
                "input_dataset",
            )

            input_dataset = Dataset(input_dataset_path)
            self.assertEqual(input_dataset.tags, {"more": "metadata"})
            self.assertEqual(input_dataset.files.one().name, "my_file.txt")

            # Check that the input manifest has been updated to use the local paths for its datasets.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "input_manifest.json")) as f:
                self.assertEqual(json.load(f)["datasets"]["input_dataset"], input_dataset_path)

            # Check the questions have been downloaded.
            with open(os.path.join(temporary_directory, self.ANALYSIS_ID, "questions.json")) as f:
                questions = json.load(f)

            self.assertEqual(questions[0]["id"], f"octue/my-child:{MOCK_SERVICE_REVISION_TAG}")

            self.assertEqual(
                questions[0]["events"],
                [
                    {"kind": "log_record", "log_record": {"msg": "Starting analysis."}},
                    {"kind": "log_record", "log_record": {"msg": "Finishing analysis."}},
                    {"kind": "monitor_message", "data": {"sample": "data"}},
                    {"kind": "result", "output_values": [1, 2, 3, 4, 5]},
                ],
            )
