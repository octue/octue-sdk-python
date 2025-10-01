import os
import uuid

from octue.twined.definitions import DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL
from octue.twined.resources import Child
from octue.twined.runner import Runner
from octue.twined.templates.template import Template


class ExampleChild(Child):
    def __init__(self):
        self.id = "local/example:latest"
        self.template = Template()

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        children=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        record_events=True,
        save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        originator=None,
        push_endpoint=None,
        asynchronous=False,
        retry_count=0,
        cpus=None,
        memory=None,
        ephemeral_storage=None,
        raise_errors=True,
        max_retries=0,
        prevent_retries_when=None,
        log_errors=True,
        timeout=86400,
        maximum_heartbeat_interval=DEFAULT_MAXIMUM_HEARTBEAT_INTERVAL,
    ):
        self.template.set_template("template-using-manifests")

        runner = Runner(
            app_src=self.template.template_path,
            twine=self.template.template_twine,
            configuration_values=os.path.join(self.template.template_path, "data", "configuration", "values.json"),
        )

        analysis = runner.run(
            input_manifest=os.path.join(self.template.template_path, "data", "input", "manifest.json")
        )

        self.template.cleanup()
        analysis.output_values = {"some": "output", "heights": [1, 2, 3, 4, 5]}

        answer = {
            "kind": "result",
            "output_values": analysis.output_values,
            "output_manifest": analysis.output_manifest,
        }

        return answer, str(uuid.uuid4())
