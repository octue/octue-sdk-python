import logging

import jsonschema

from octue.compatibility import warn_if_incompatible


logger = logging.getLogger(__name__)

SERVICE_COMMUNICATION_SCHEMA = "https://jsonschema.registry.octue.com/octue/service-communication/0.2.0.json"
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"


def validate_message(message, attributes, schema):
    jsonschema.validate({"data": message, "attributes": dict(attributes)}, schema)


def log_invalid_message(message, receiving_service, parent_sdk_version, child_sdk_version):
    """Log an invalid message and issue a warning if the parent and child SDK versions are incompatible.

    :param dict message: the invalid message
    :param octue.cloud.pub_sub.service.Service receiving_service: the service that received the invalid message
    :param str parent_sdk_version: the semantic version of Octue SDK running the parent
    :param str child_sdk_version: the semantic version of Octue SDK running the child
    :return None:
    """
    warn_if_incompatible(parent_sdk_version=parent_sdk_version, child_sdk_version=child_sdk_version)

    logger.exception(
        "%r received a message that doesn't conform with the service communication schema (%s): %r.",
        receiving_service,
        SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
        message,
    )
