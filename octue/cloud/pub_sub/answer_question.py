import logging

from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import create_sruid, get_sruid_parts
from octue.configuration import load_service_and_app_configuration
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner
from octue.utils.objects import get_nested_attribute

logger = logging.getLogger(__name__)


def answer_pub_sub_question(question, project_name, service_configuration=None, app_configuration=None):
    """Answer a question received by a service via Pub/Sub.

    :param dict question: a question event and its attributes
    :param str project_name: the name of the project the service is running on
    :param octue.configuration.ServiceConfiguration|None service_configuration:
    :param octue.configuration.AppConfiguration|None app_configuration:
    :return None:
    """
    if not service_configuration:
        service_configuration, app_configuration = load_service_and_app_configuration()

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

    except BaseException as error:  # noqa
        service.send_exception(
            question_uuid=question_uuid,
            parent_question_uuid=get_nested_attribute(question, "attributes.parent_question_uuid"),
            originator_question_uuid=get_nested_attribute(question, "attributes.originator_question_uuid"),
            parent=get_nested_attribute(question, "attributes.parent"),
            originator=get_nested_attribute(question, "attributes.originator"),
            retry_count=get_nested_attribute(question, "attributes.retry_count"),
        )

        logger.exception(error)
