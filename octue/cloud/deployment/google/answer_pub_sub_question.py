import logging

from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import create_service_sruid, get_service_sruid_parts
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
    service_namespace, service_name, service_revision_tag = get_service_sruid_parts(service_configuration)

    service_sruid = create_service_sruid(
        namespace=service_namespace,
        name=service_name,
        revision_tag=service_revision_tag,
    )

    service = Service(service_id=service_sruid, backend=GCPPubSubBackend(project_name=project_name))

    question_uuid = get_nested_attribute(question, "attributes.question_uuid")
    answer_topic = service.instantiate_answer_topic(question_uuid)

    try:
        runner = Runner(
            app_src=service_configuration.app_source_path,
            twine=service_configuration.twine_path,
            configuration_values=app_configuration.configuration_values,
            configuration_manifest=app_configuration.configuration_manifest,
            children=app_configuration.children,
            output_location=app_configuration.output_location,
            crash_diagnostics_cloud_path=service_configuration.crash_diagnostics_cloud_path,
            project_name=project_name,
            service_id=service_sruid,
        )

        service.run_function = runner.run

        service.answer(question, answer_topic=answer_topic)
        logger.info("Analysis successfully run and response sent for question %r.", question_uuid)

    # Forward any errors in the deployment configuration (errors in the analysis are already forwarded by the service).
    except BaseException as error:  # noqa
        service.send_exception(topic=answer_topic)
        logger.exception(error)
