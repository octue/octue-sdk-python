import logging

import jsonschema

from octue.compatibility import warn_if_incompatible


logger = logging.getLogger(__name__)

SERVICE_COMMUNICATION_SCHEMA = "https://jsonschema.registry.octue.com/octue/service-communication/0.6.0.json"
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"


def is_event_valid(event, attributes, receiving_service, parent_sdk_version, child_sdk_version, schema=None):
    """Check if the event or its attributes are valid according to the schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param octue.cloud.pub_sub.service.Service receiving_service: the service that received the event and is validating it
    :param str parent_sdk_version: the semantic version of Octue SDK running the parent
    :param str child_sdk_version: the semantic version of Octue SDK running the child
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
    """Raise an error if the event or its attributes aren't valid according to the schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param octue.cloud.pub_sub.service.Service receiving_service: the service that received the event and is validating it
    :param str parent_sdk_version: the semantic version of Octue SDK running the parent
    :param str child_sdk_version: the semantic version of Octue SDK running the child
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :raise jsonschema.ValidationError: if the event or its attributes are invalid
    :return None:
    """
    if schema is None:
        schema = {"$ref": SERVICE_COMMUNICATION_SCHEMA}

    try:
        jsonschema.validate({"event": event, "attributes": dict(attributes)}, schema)
    except jsonschema.ValidationError as error:
        warn_if_incompatible(parent_sdk_version=parent_sdk_version, child_sdk_version=child_sdk_version)

        logger.exception(
            "%r received an event that doesn't conform with the service communication schema (%s): %r.",
            receiving_service,
            SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
            event,
        )

        raise error