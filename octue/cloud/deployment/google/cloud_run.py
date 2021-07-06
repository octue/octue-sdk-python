import functools
import json
import logging
import os
from flask import Flask, request

from octue.cloud.pub_sub.service import Service
from octue.exceptions import MissingServiceID
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner


DEPLOYMENT_CONFIGURATION_PATH = "deployment_configuration.json"


logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """Receive questions from Google Cloud Run in the form of Google Pub/Sub messages.

    :return (str, int):
    """
    envelope = request.get_json()

    if not envelope:
        return _log_bad_request_and_return_400_response("No Pub/Sub message received.")

    if not isinstance(envelope, dict) or "message" not in envelope:
        return _log_bad_request_and_return_400_response("Invalid Pub/Sub message format.")

    question = envelope["message"]

    if "data" not in question or "attributes" not in question or "question_uuid" not in question["attributes"]:
        return _log_bad_request_and_return_400_response("Invalid Pub/Sub message format.")

    project_name = envelope["subscription"].split("/")[1]
    answer_question(project_name=project_name, question=question)
    return ("", 204)


def _log_bad_request_and_return_400_response(message):
    """Log an error return a bad request (400) response.

    :param str message:
    :return (str, int):
    """
    logger.error(message)
    return (f"Bad Request: {message}", 400)


def answer_question(project_name, question, credentials_environment_variable=None):
    """Answer a question from a service by running the deployed app with the deployment configuration. Either the
    `deployment_configuration_path` should be specified, or the `deployment_configuration`.

    :param str project_name:
    :param str credentials_environment_variable:
    :return None:
    """
    service_id = os.environ.get("SERVICE_ID")

    if not service_id:
        raise MissingServiceID(
            "The ID for the deployed service is missing - ensure SERVICE_ID is available as an environment variable."
        )

    deployment_configuration = _get_deployment_configuration(DEPLOYMENT_CONFIGURATION_PATH)

    runner = Runner(
        app_src=deployment_configuration.get("app_dir", "."),
        twine=deployment_configuration.get("twine", "twine.json"),
        configuration_values=deployment_configuration.get("configuration_values", None),
        configuration_manifest=deployment_configuration.get("configuration_manifest", None),
        output_manifest_path=deployment_configuration.get("output_manifest", None),
        children=deployment_configuration.get("children", None),
        skip_checks=deployment_configuration.get("skip_checks", False),
        project_name=project_name,
    )

    run_function = functools.partial(
        runner.run,
        analysis_log_level=deployment_configuration.get("log_level", "INFO"),
        analysis_log_handler=deployment_configuration.get("log_handler", None),
    )

    service = Service(
        service_id=service_id,
        backend=GCPPubSubBackend(
            project_name=project_name, credentials_environment_variable=credentials_environment_variable
        ),
        run_function=run_function,
    )

    try:
        service.answer(question)
        logger.info(
            "Analysis successfully run and response sent for question %r.", question["attributes"]["question_uuid"]
        )
    except BaseException as error:  # noqa
        logger.exception(error)


def _get_deployment_configuration(deployment_configuration_path):
    """Get the deployment configuration from the given JSON file path or return an empty one.

    :param str deployment_configuration_path: path to deployment configuration file
    :return dict:
    """
    try:
        with open(deployment_configuration_path) as f:
            deployment_configuration = json.load(f)

        logger.info("Deployment configuration loaded from %r.", os.path.abspath(deployment_configuration_path))

    except FileNotFoundError:
        deployment_configuration = {}
        logger.info("Default deployment configuration used.")

    return deployment_configuration
