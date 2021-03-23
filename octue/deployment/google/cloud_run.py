import base64
import json
import logging
import os
from flask import Flask, request

from octue.logging_handlers import apply_log_handler
from octue.resources.communication.google_pub_sub.service import Service
from octue.resources.communication.service_backends import GCPPubSubBackend
from octue.runner import Runner


logger = logging.getLogger(__name__)
apply_log_handler(logger, log_level=logging.INFO)

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    logger.info(request)
    logger.info(vars(request))
    envelope = request.get_json()
    logger.info(envelope)

    if not envelope:
        message = "No Pub/Sub message received."
        logger.error(message)
        return f"Bad Request: {message}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        message = "Invalid Pub/Sub message format."
        logger.error(message)
        return f"Bad Request: {message}", 400

    pubsub_message = envelope["message"]
    data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8").strip())
    question_uuid = pubsub_message["attributes"]["question_uuid"]
    logger.info("Received question %r.", question_uuid)

    run_analysis(data, question_uuid)
    logger.info("Analysis run and response sent for question %r.", question_uuid)
    return ("", 204)


def run_analysis(
    data, question_uuid, deployment_configuration_path="deployment_configuration.json", deployment_configuration=None
):
    """Run an analysis on the given data using the app with the deployment configuration.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    if not deployment_configuration:
        with open(deployment_configuration_path) as f:
            deployment_configuration = json.load(f)

    runner = Runner(
        app_src=deployment_configuration.get("app_dir", "."),
        twine=deployment_configuration.get("twine", "twine.json"),
        configuration_values=deployment_configuration.get("configuration_values", None),
        configuration_manifest=deployment_configuration.get("configuration_manifest", None),
        output_manifest_path=deployment_configuration.get("output_manifest", None),
        children=deployment_configuration.get("children", None),
        skip_checks=deployment_configuration.get("skip_checks", False),
        log_level=deployment_configuration.get("log_level", "INFO"),
        handler=deployment_configuration.get("log_handler", None),
        show_twined_logs=deployment_configuration.get("show_twined_logs", False),
    )

    service = Service(
        id=os.environ["SERVICE_ID"],
        backend=GCPPubSubBackend(project_name=os.environ["PROJECT_ID"], credentials_environment_variable=None),
        run_function=runner.run,
    )

    service.answer(data=data, question_uuid=question_uuid)
