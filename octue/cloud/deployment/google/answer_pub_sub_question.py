import functools
import logging
import os

from octue.cloud.pub_sub.service import Service
from octue.configuration import load_service_and_app_configuration
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner
from octue.utils.objects import get_nested_attribute


logger = logging.getLogger(__name__)


DEFAULT_SERVICE_CONFIGURATION_PATH = "octue.yaml"


def answer_question(question, project_name):
    """Answer a question sent to an app deployed in Google Cloud.

    :param dict|tuple|apache_beam.io.gcp.pubsub.PubsubMessage question:
    :param str project_name:
    :return None:
    """
    service_configuration, app_configuration = load_service_and_app_configuration(DEFAULT_SERVICE_CONFIGURATION_PATH)
    service_id = os.environ.get("SERVICE_ID") or service_configuration.service_id

    service = Service(
        service_id=service_id,
        backend=GCPPubSubBackend(project_name=project_name),
    )

    question_uuid = get_nested_attribute(question, "attributes.question_uuid")
    answer_topic = service.instantiate_answer_topic(question_uuid)

    try:
        service.name = service_configuration.name

        runner = Runner(
            app_src=service_configuration.app_source_path,
            twine=service_configuration.twine_path,
            configuration_values=app_configuration.configuration_values,
            configuration_manifest=app_configuration.configuration_manifest,
            children=app_configuration.children,
            output_location=app_configuration.output_location,
            project_name=project_name,
            service_id=service_id,
        )

        service.run_function = functools.partial(runner.run)

        service.answer(question, answer_topic=answer_topic)
        logger.info("Analysis successfully run and response sent for question %r.", question_uuid)

    # Forward any errors in the deployment configuration (errors in the analysis are already forwarded by the service).
    except BaseException as error:  # noqa
        service.send_exception(topic=answer_topic)
        logger.exception(error)
