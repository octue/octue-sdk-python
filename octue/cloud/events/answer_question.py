import logging

from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import create_sruid, get_sruid_parts
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner

logger = logging.getLogger(__name__)


def answer_question(question, project_name, service_configuration, app_configuration):
    """Answer a question received by a service.

    :param dict question: a question event and its attributes
    :param str project_name: the name of the project the service is running on
    :param octue.configuration.ServiceConfiguration service_configuration:
    :param octue.configuration.AppConfiguration app_configuration:
    :return dict: the result event
    """
    service_namespace, service_name, service_revision_tag = get_sruid_parts(service_configuration)
    service_sruid = create_sruid(namespace=service_namespace, name=service_name, revision_tag=service_revision_tag)
    service = Service(service_id=service_sruid, backend=GCPPubSubBackend(project_name=project_name))

    runner = Runner.from_configuration(
        service_configuration=service_configuration,
        app_configuration=app_configuration,
        project_name=project_name,
        service_id=service_sruid,
    )

    service.run_function = runner.run
    return service.answer(question)
