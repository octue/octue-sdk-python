import os

from octue.twined.resources import Child
from octue.twined.runner import Runner
from octue.twined.templates.template import Template


class ExampleChild(Child):
    def __init__(self, id, backend):
        pass

    def ask(self, *args, **kwargs):
        template = Template()
        template.set_template("template-using-manifests")

        runner = Runner(
            app_src=template.template_path,
            twine=template.template_twine,
            configuration_values=os.path.join(template.template_path, "data", "configuration", "values.json"),
        )

        analysis = runner.run(input_manifest=os.path.join(template.template_path, "data", "input", "manifest.json"))

        template.cleanup()
        analysis.output_values = {"some": "output", "heights": [1, 2, 3, 4, 5]}

        answer = {
            "kind": "result",
            "output_values": analysis.output_values,
            "output_manifest": analysis.output_manifest,
        }

        return answer, analysis.id
