import json
import os

from octue.resources.communication.google_pub_sub.service import Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from octue.runner import Runner


def run_analysis(event, context):
    """Run an analysis on the given data using the app with the deployment configuration.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    with open("deployment_configuration.json") as f:
        deployment_configuration = json.load(f)

    runner = Runner(
        app_src=deployment_configuration["app_dir"],
        twine=deployment_configuration["twine"],
        configuration_values=deployment_configuration.get("configuration_values", None),
        configuration_manifest=deployment_configuration.get("configuration_manifest", None),
        output_manifest_path=deployment_configuration.get("output_manifest", None),
        children=deployment_configuration.get("children", None),
        skip_checks=deployment_configuration.get("skip_checks", False),
        log_level=deployment_configuration.get("log_level", "info"),
        handler=deployment_configuration.get("log_handler", None),
        show_twined_logs=deployment_configuration.get("show_twined_logs", False),
    )

    service = Service(
        id=event["attributes"]["id"],
        backend=GCPPubSubBackend(project_name=os.environ["GCP_PROJECT"]),
        run_function=runner.run,
    )

    service.answer(data=event["data"], question_uuid=event["attributes"]["question_uuid"])
