import logging

from octue.cloud.pub_sub import Topic
from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import convert_service_id_to_pub_sub_form, create_sruid, get_sruid_parts
from octue.configuration import load_service_and_app_configuration
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner
from octue.utils.objects import get_nested_attribute


logger = logging.getLogger(__name__)


DEFAULT_SERVICE_CONFIGURATION_PATH = "octue.yaml"


def answer_question(question, project_name):
    """Answer a question sent to an app deployed in Google Cloud.

    :param dict|tuple question:
    :param str project_name:
    :return None:
    """
    service_configuration, app_configuration = load_service_and_app_configuration(DEFAULT_SERVICE_CONFIGURATION_PATH)
    service_namespace, service_name, service_revision_tag = get_sruid_parts(service_configuration)

    service_sruid = create_sruid(
        namespace=service_namespace,
        name=service_name,
        revision_tag=service_revision_tag,
    )

    service = Service(service_id=service_sruid, backend=GCPPubSubBackend(project_name=project_name))
    question_uuid = get_nested_attribute(question, "attributes.question_uuid")

    try:
        runner = Runner.from_configuration(
            service_configuration=service_configuration,
            app_configuration=app_configuration,
            project_name=project_name,
            service_id=service_sruid,
        )

        service.run_function = runner.run
        service.answer(question)
        logger.info("Analysis successfully run and response sent for question %r.", question_uuid)

    except BaseException as error:  # noqa
        service.send_exception(
            topic=Topic(name=convert_service_id_to_pub_sub_form(service_sruid), project_name=project_name),
            question_uuid=question_uuid,
        )

        logger.exception(error)
