import logging
import os

import jsonschema

from octue.compatibility import warn_if_incompatible


logger = logging.getLogger(__name__)

SERVICE_COMMUNICATION_SCHEMA = {"$ref": "https://jsonschema.registry.octue.com/octue/service-communication/0.8.2.json"}
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"
SERVICE_COMMUNICATION_SCHEMA_VERSION = os.path.splitext(SERVICE_COMMUNICATION_SCHEMA["$ref"])[0].split("/")[-1]

# Instantiate a JSON schema validator to cache the service communication schema. This avoids getting it from the
# registry every time a message is validated against it.
jsonschema.Draft202012Validator.check_schema(SERVICE_COMMUNICATION_SCHEMA)
jsonschema_validator = jsonschema.Draft202012Validator(SERVICE_COMMUNICATION_SCHEMA)


def is_event_valid(event, attributes, receiving_service, parent_sdk_version, child_sdk_version, schema=None):
    """Check if the event and its attributes are valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param octue.cloud.pub_sub.service.Service receiving_service: the service receiving and validating the event
    :param str parent_sdk_version: the semantic version of Octue SDK running on the parent
    :param str child_sdk_version: the semantic version of Octue SDK running on the child
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :return bool: `True` if the event and its attributes are valid
    """
    try:
        raise_if_event_is_invalid(
            event,
            attributes,
            receiving_service,
            parent_sdk_version,
            child_sdk_version,
            schema=schema,
        )
    except jsonschema.ValidationError:
        return False

    return True


def raise_if_event_is_invalid(
    event,
    attributes,
    receiving_service,
    parent_sdk_version,
    child_sdk_version,
    schema=None,
):
    """Raise an error if the event or its attributes aren't valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param octue.cloud.pub_sub.service.Service receiving_service: the service receiving and validating the event
    :param str parent_sdk_version: the semantic version of Octue SDK running on the parent
    :param str child_sdk_version: the semantic version of Octue SDK running on the child
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :raise jsonschema.ValidationError: if the event or its attributes are invalid
    :return None:
    """
    data = {"event": event, "attributes": dict(attributes)}

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
        warn_if_incompatible(parent_sdk_version=parent_sdk_version, child_sdk_version=child_sdk_version)

        logger.exception(
            "%r received an event that doesn't conform with version %s of the service communication schema (%s): %r.",
            receiving_service,
            SERVICE_COMMUNICATION_SCHEMA_VERSION,
            SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
            event,
        )

        raise error
