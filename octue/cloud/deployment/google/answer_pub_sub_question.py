import functools
import json
import logging
import os

from octue.cloud.pub_sub.service import Service
from octue.exceptions import MissingServiceID
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner
from octue.utils.objects import get_nested_attribute


logger = logging.getLogger(__name__)


DEPLOYMENT_CONFIGURATION_PATH = "deployment_configuration.json"


def answer_question(question, project_name, credentials_environment_variable="GOOGLE_APPLICATION_CREDENTIALS"):
    """Answer a question from a service by running the deployed app with the deployment configuration. Either the
    `deployment_configuration_path` should be specified, or the `deployment_configuration`.

    :param dict|tuple|apache_beam.io.gcp.pubsub.PubsubMessage question:
    :param str project_name:
    :param str credentials_environment_variable:
    :return None:
    """
    service_id = os.environ.get("SERVICE_ID")

    if not service_id:
        raise MissingServiceID(
            "The ID for the deployed service is missing or empty - ensure SERVICE_ID is available as an environment "
            "variable."
        )

    question_uuid = get_nested_attribute(question, "attributes.question_uuid")

    service = Service(
        service_id=service_id,
        backend=GCPPubSubBackend(
            project_name=project_name, credentials_environment_variable=credentials_environment_variable
        ),
        name=os.environ.get("SERVICE_NAME"),
    )

    answer_topic = service.instantiate_answer_topic(question_uuid)

    try:
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

        service.run_function = functools.partial(
            runner.run,
            analysis_log_level=deployment_configuration.get("log_level", "INFO"),
            analysis_log_handler=deployment_configuration.get("log_handler", None),
        )

        service.answer(question, answer_topic=answer_topic)
        logger.info("Analysis successfully run and response sent for question %r.", question_uuid)

    # Forward any errors in the deployment configuration (errors in the analysis are already forwarded by the service).
    except BaseException as error:  # noqa
        service.send_exception_to_asker(topic=answer_topic)
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
