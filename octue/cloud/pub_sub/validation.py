import logging

import jsonschema

from octue.compatibility import warn_if_incompatible


logger = logging.getLogger(__name__)

SERVICE_COMMUNICATION_SCHEMA = "https://jsonschema.registry.octue.com/octue/service-communication/0.1.4.json"
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"


def warn_of_or_raise_invalid_message_error(message, error, receiving_service, parent_sdk_version, child_sdk_version):
    """Log the error if it's due to an invalid message or raise it if it's due to anything else. Issue an additional
    warning if the parent and child SDK versions are incompatible.

    :param dict message: the message whose handling has caused an error
    :param Exception error: the error caused by handling the message
    :return None:
    """
    warn_if_incompatible(parent_sdk_version=parent_sdk_version, child_sdk_version=child_sdk_version)

    # Just log a warning if an invalid message type has been received - the service should continue to run.
    if isinstance(error, jsonschema.ValidationError):
        logger.exception(
            "%r received a message that doesn't conform with the service communication schema (%s): %r.",
            receiving_service,
            SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
            message,
        )
        return

    # Raise all other errors.
    raise error
