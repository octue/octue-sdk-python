import logging

import jsonschema

from octue.compatibility import warn_if_incompatible

VALID_EVENT_KINDS = {
    "question",
    "delivery_acknowledgement",
    "heartbeat",
    "log_record",
    "monitor_message",
    "exception",
    "result",
}

SERVICE_COMMUNICATION_SCHEMA_VERSION = "0.14.1"
SERVICE_COMMUNICATION_SCHEMA_INFO_URL = "https://strands.octue.com/octue/service-communication"

# SERVICE_COMMUNICATION_SCHEMA = {
#     "$ref": f"https://jsonschema.registry.octue.com/octue/service-communication/{SERVICE_COMMUNICATION_SCHEMA_VERSION}.json"
# }


SERVICE_COMMUNICATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Octue services communication",
    "description": "A schema describing the events Octue services can emit and consume.",
    "type": "object",
    "properties": {
        "attributes": {
            "title": "Event attributes",
            "description": "Metadata for routing the event, adding context, and guiding the receiver's behaviour.",
            "type": "object",
            "oneOf": [
                {
                    "title": "Attributes for an event from a parent service",
                    "properties": {
                        "datetime": {
                            "type": "string",
                            "format": "date-time",
                            "description": "The UTC datetime the event was emitted at in ISO8601 format.",
                        },
                        "uuid": {
                            "type": "string",
                            "format": "uuid",
                            "description": "A universally unique identifier for this event.",
                        },
                        "question_uuid": {
                            "type": "string",
                            "description": "The UUID of the question the event is related to.",
                        },
                        "parent_question_uuid": {
                            "oneOf": [
                                {
                                    "type": "string",
                                    "description": "The UUID of the question that triggered this question.",
                                },
                                {"type": "null", "description": "If this is the originating question."},
                            ]
                        },
                        "originator_question_uuid": {
                            "type": "string",
                            "description": "The UUID of the ultimate question that triggered this tree of questions.",
                        },
                        "forward_logs": {"oneOf": [{"type": "boolean"}, {"enum": ["0", "1"]}]},
                        "save_diagnostics": {
                            "enum": ["SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"]
                        },
                        "parent": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the parent that asked the question this event is related to.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "originator": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision that triggered the tree of questions this event is related to.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "sender": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision emitting the event.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "sender_type": {
                            "type": "string",
                            "pattern": "^PARENT$",
                            "description": "An indicator that the sender is a parent.",
                        },
                        "sender_sdk_version": {
                            "type": "string",
                            "description": "The version of Octue SDK the sender is running.",
                        },
                        "recipient": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision this event is meant for.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "retry_count": {
                            "type": "integer",
                            "description": "The retry count for the question. All events related to the retry of a given question will have the same retry count. A question that is being asked for the first time will have a retry count of 0.",
                            "minimum": 0,
                        },
                    },
                    "required": [
                        "datetime",
                        "uuid",
                        "question_uuid",
                        "parent_question_uuid",
                        "originator_question_uuid",
                        "forward_logs",
                        "save_diagnostics",
                        "parent",
                        "originator",
                        "sender",
                        "sender_type",
                        "sender_sdk_version",
                        "recipient",
                        "retry_count",
                    ],
                },
                {
                    "title": "Attributes for an event from a child service",
                    "properties": {
                        "datetime": {
                            "type": "string",
                            "format": "date-time",
                            "description": "The UTC datetime the event was emitted at in ISO8601 format.",
                        },
                        "uuid": {
                            "type": "string",
                            "format": "uuid",
                            "description": "A universally unique identifier for this event.",
                        },
                        "question_uuid": {
                            "type": "string",
                            "description": "The UUID of the question the event is related to.",
                        },
                        "parent_question_uuid": {
                            "oneOf": [
                                {
                                    "type": "string",
                                    "description": "The UUID of the question that triggered this question.",
                                },
                                {"type": "null", "description": "If this is the originating question."},
                            ]
                        },
                        "originator_question_uuid": {
                            "type": "string",
                            "description": "The UUID of the ultimate question that triggered this tree of questions.",
                        },
                        "parent": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the parent that asked the question this event is related to.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "originator": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision that triggered the tree of questions this event is related to.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "sender": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision emitting the event.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "sender_type": {
                            "type": "string",
                            "pattern": "^CHILD$",
                            "description": "An indicator that the sender is a child.",
                        },
                        "sender_sdk_version": {
                            "type": "string",
                            "description": "The version of Octue SDK the sender is running.",
                        },
                        "recipient": {
                            "type": "string",
                            "description": "The service revision unique identifier (SRUID) of the service revision this event is meant for.",
                            "examples": ["octue:test-service:1.2.0"],
                        },
                        "retry_count": {
                            "type": "integer",
                            "description": "The retry count for the question. All events related to the retry of a given question will have the same retry count. A question that is being asked for the first time will have a retry count of 0.",
                            "minimum": 0,
                        },
                    },
                    "required": [
                        "datetime",
                        "uuid",
                        "question_uuid",
                        "parent_question_uuid",
                        "originator_question_uuid",
                        "parent",
                        "originator",
                        "sender",
                        "sender_type",
                        "sender_sdk_version",
                        "recipient",
                        "retry_count",
                    ],
                },
            ],
        },
        "event": {
            "title": "Event data",
            "description": "An Octue service event/message (e.g. heartbeat, log record, result).",
            "type": "object",
            "oneOf": [
                {
                    "title": "Delivery acknowledgement",
                    "description": "An acknowledgement of successful receipt of a question. This type of message can only be sent by a child to a parent as part of the child's response to a question.",
                    "type": "object",
                    "properties": {"kind": {"type": "string", "pattern": "^delivery_acknowledgement$"}},
                    "required": ["kind"],
                },
                {
                    "title": "Heartbeat",
                    "type": "object",
                    "description": "A message sent at regular intervals to let the parent know the child is still processing its question and that it should keep waiting for further messages. This type of message can only be sent by a child to a parent as part of the child's response to a question.",
                    "properties": {"kind": {"type": "string", "pattern": "^heartbeat$"}},
                    "required": ["kind"],
                },
                {
                    "title": "Monitor message",
                    "type": "object",
                    "description": "An interim result or update sent during the processing of a question. This type of message can only be sent by a child to a parent as part of the child's response to a question.",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^monitor_message$"},
                        "data": {
                            "description": "This schema is set in the child's twine (see https://twined.readthedocs.io/en/latest/anatomy_monitors.html)."
                        },
                    },
                    "required": ["kind", "data"],
                },
                {
                    "title": "Log record",
                    "description": "A log record generated during the processing of a question. This type of message can only be sent by a child to a parent as part of the child's response to a question.",
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^log_record$"},
                        "log_record": {"type": "object"},
                    },
                    "required": ["kind", "log_record"],
                },
                {
                    "title": "Exception",
                    "description": "An unhandled error raised during the processing of a question, marking its premature end. This type of message can only be sent by a child to a parent as part of the child's response to a question.",
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^exception$"},
                        "exception_message": {"type": "string"},
                        "exception_type": {"type": "string"},
                        "exception_traceback": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["kind", "exception_message", "exception_type", "exception_traceback"],
                },
                {
                    "title": "Result",
                    "description": "The final result of processing a question. This type of message can only and must be sent by a child to a parent to complete the child's response to a question.",
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^result$"},
                        "output_values": {
                            "description": "This schema is set in the child's twine (see https://twined.readthedocs.io/en/latest/anatomy_values.html)."
                        },
                        "output_manifest": {
                            "description": "See schema information here: https://strands.octue.com/octue/manifest",
                            "$ref": "https://jsonschema.registry.octue.com/octue/manifest/0.1.0.json",
                        },
                    },
                    "required": ["kind"],
                },
                {
                    "title": "Question",
                    "description": "A question for a child to process. This type of message can only be sent by a parent to a child to trigger the child to process a question.",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^question$"},
                        "input_values": {
                            "description": "This schema is set in the child's twine (see https://twined.readthedocs.io/en/latest/anatomy_values.html)."
                        },
                        "input_manifest": {
                            "description": "See schema information here: https://strands.octue.com/octue/manifest",
                            "$ref": "https://jsonschema.registry.octue.com/octue/manifest/0.1.0.json",
                        },
                        "children": {
                            "description": "See schema information here: https://strands.octue.com/octue/children",
                            "$ref": "https://jsonschema.registry.octue.com/octue/children/0.1.0.json",
                        },
                    },
                    "required": ["kind"],
                },
                {
                    "title": "Cancellation",
                    "description": "A cancellation of a question. This type of message can only be sent by a parent.",
                    "properties": {
                        "kind": {"type": "string", "pattern": "^cancellation$"},
                    },
                    "required": ["kind"],
                },
            ],
        },
    },
    "required": ["attributes", "event"],
}


# Instantiate a JSON schema validator to cache the service communication schema. This avoids downloading it from the
# registry every time a message is validated against it.
jsonschema.Draft202012Validator.check_schema(SERVICE_COMMUNICATION_SCHEMA)
jsonschema_validator = jsonschema.Draft202012Validator(SERVICE_COMMUNICATION_SCHEMA)

logger = logging.getLogger(__name__)


def is_event_valid(event, attributes, recipient, parent_sdk_version, child_sdk_version, schema=None):
    """Check if the event and its attributes are valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param str recipient: the SRUID of the service revision receiving and validating the event
    :param str parent_sdk_version: the semantic version of Octue SDK running on the parent
    :param str child_sdk_version: the semantic version of Octue SDK running on the child
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :return bool: `True` if the event and its attributes are valid
    """
    try:
        raise_if_event_is_invalid(
            event,
            attributes,
            recipient,
            parent_sdk_version,
            child_sdk_version,
            schema=schema,
        )
    except jsonschema.ValidationError:
        return False

    return True


def raise_if_event_is_invalid(event, attributes, recipient, parent_sdk_version, child_sdk_version, schema=None):
    """Raise an error if the event or its attributes aren't valid according to the Octue services communication schema.

    :param dict event: the event to validate
    :param dict attributes: the attributes of the event to validate
    :param str recipient: the SRUID of the service revision receiving and validating the event
    :param str parent_sdk_version: the semantic version of Octue SDK running on the parent
    :param str child_sdk_version: the semantic version of Octue SDK running on the child
    :param dict|None schema: the schema to validate the event and its attributes against; if `None`, this defaults to the service communication schema used in this version of Octue SDK
    :raise jsonschema.ValidationError: if the event or its attributes are invalid
    :return None:
    """
    # Transform attributes to a dictionary in the case they're a different kind of mapping.
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
            recipient,
            SERVICE_COMMUNICATION_SCHEMA_VERSION,
            SERVICE_COMMUNICATION_SCHEMA_INFO_URL,
            event,
        )

        raise error
