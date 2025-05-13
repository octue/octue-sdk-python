import logging

from octue.cloud.pub_sub.service import Service
from octue.cloud.service_id import create_sruid, get_sruid_parts
from octue.resources.service_backends import GCPPubSubBackend
from octue.runner import Runner

logger = logging.getLogger(__name__)


def answer_question(question, project_id, service_configuration):
    """Answer a question received by a service.

    :param dict question: a question event and its attributes
    :param str project_id: the ID of the project the service is running on
    :param octue.configuration.ServiceConfiguration service_configuration:
    :return dict: the result event
    """
    service_namespace, service_name, service_revision_tag = get_sruid_parts(service_configuration)
    service_sruid = create_sruid(namespace=service_namespace, name=service_name, revision_tag=service_revision_tag)
    service = Service(service_id=service_sruid, backend=GCPPubSubBackend(project_id=project_id))

    runner = Runner.from_configuration(
        service_configuration=service_configuration,
        project_id=project_id,
        service_id=service_sruid,
    )

    service.run_function = runner.run
    return service.answer(question)
