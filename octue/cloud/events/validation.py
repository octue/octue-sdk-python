import logging

import jsonschema

from octue.compatibility import warn_if_incompatible
from octue.definitions import LOCAL_SDK_VERSION

VALID_EVENT_KINDS = {
    "question",
    "delivery_acknowledgement",
    "heartbeat",
    "log_record",
    "monitor_message",
    "exception",
    "result",
}

SERVICE_COMMUNICATION_SCHEMA_VERSION = "0.15.0"
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"

SERVICE_COMMUNICATION_SCHEMA = {
    "$ref": f"https://jsonschema.registry.octue.com/octue/service-communication/{SERVICE_COMMUNICATION_SCHEMA_VERSION}.json"
}

# Instantiate a JSON schema validator to cache the service communication schema. This avoids downloading it from the
# registry every time a message is validated against it.
jsonschema.Draft202012Validator.check_schema(SERVICE_COMMUNICATION_SCHEMA)
jsonschema_validator = jsonschema.Draft202012Validator(SERVICE_COMMUNICATION_SCHEMA)

logger = logging.getLogger(__name__)


def is_event_valid(event, attributes, recipient, schema=None):
    """Check if the event and its attributes are valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param octue.cloud.events.attributes.EventAttributes attributes: the attributes of the event to validate
    :param str recipient: the SRUID of the service revision receiving and validating the event
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :return bool: `True` if the event and its attributes are valid
    """
    try:
        raise_if_event_is_invalid(event, attributes, recipient, schema=schema)
    except jsonschema.ValidationError:
        return False

    return True


def raise_if_event_is_invalid(event, attributes, recipient, schema=None):
    """Raise an error if the event or its attributes aren't valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param octue.cloud.events.attributes.EventAttributes attributes: the attributes of the event to validate
    :param str recipient: the SRUID of the service revision receiving and validating the event
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :raise jsonschema.ValidationError: if the event or its attributes are invalid
    :return None:
    """
    # Transform attributes to a dictionary in the case they're a different kind of mapping.
    data = {"event": event, "attributes": attributes.to_minimal_dict()}

    if schema is None:
        schema = SERVICE_COMMUNICATION_SCHEMA

    try:
        # If the schema is the official service communication schema, use the cached validator.
        if schema == SERVICE_COMMUNICATION_SCHEMA:
            jsonschema_validator.validate(data)

        # Otherwise, use uncached validation.
        else:
            jsonschema.validate(data, schema)

    except jsonschema.ValidationError as error:
        warn_if_incompatible(sender_sdk_version=attributes.sender_sdk_version, recipient_sdk_version=LOCAL_SDK_VERSION)

        logger.exception(
            "%r received an event that doesn't conform with version %s of the service communication schema (%s): %r.",
            recipient,
            SERVICE_COMMUNICATION_SCHEMA_VERSION,
            SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
            event,
        )

        raise error
