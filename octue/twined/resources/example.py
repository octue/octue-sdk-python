import os

from octue.twined.templates.template import Template


def run_example():
    # Avoid circular import.
    from octue.twined.runner import Runner

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
