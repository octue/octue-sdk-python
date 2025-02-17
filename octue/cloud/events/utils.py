import datetime
import uuid

from octue.definitions import LOCAL_SDK_VERSION
from octue.utils.dictionaries import make_minimal_dictionary


def make_question_event(
    input_values,
    input_manifest,
    sender=None,
    recipient=None,
    question_uuid=None,
    attributes=None,
):
    """Make a question event. If the `attributes` argument isn't provided, the question will be an originator question.

    :param dict input_values:
    :param octue.resources.manifest.Manifest input_manifest:
    :param str|None sender:
    :param str|None recipient:
    :param str|None question_uuid:
    :param dict|None attributes:
    :return dict:
    """
    if not attributes:
        question_uuid = question_uuid or str(uuid.uuid4())

        attributes = make_attributes(
            question_uuid=question_uuid,
            sender=sender,
            recipient=recipient,
            forward_logs=True,
            save_diagnostics="SAVE_DIAGNOSTICS_ON",
            sender_type="PARENT",
        )

    return {
        "event": make_minimal_dictionary(input_values=input_values, input_manifest=input_manifest, kind="question"),
        "attributes": attributes,
    }


def make_attributes(
    sender,
    sender_type,
    recipient,
    question_uuid=None,
    parent_question_uuid=None,
    originator_question_uuid=None,
    parent=None,
    originator=None,
    retry_count=0,
    forward_logs=None,
    save_diagnostics=None,
    cpus=None,
    memory=None,
    ephemeral_storage=None,
):
    # If the originator isn't provided, assume that this service revision is the originator.
    originator_question_uuid = originator_question_uuid or question_uuid
    parent = parent or sender
    originator = originator or sender

    attributes = {
        "uuid": str(uuid.uuid4()),
        "datetime": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "question_uuid": question_uuid or str(uuid.uuid4()),
        "parent_question_uuid": parent_question_uuid,
        "originator_question_uuid": originator_question_uuid,
        "parent": parent,
        "originator": originator,
        "sender": sender,
        "sender_type": sender_type,
        "sender_sdk_version": LOCAL_SDK_VERSION,
        "recipient": recipient,
        "retry_count": int(retry_count),
    }

    if sender_type == "PARENT":
        if forward_logs:
            attributes["forward_logs"] = bool(forward_logs)

        attributes.update(
            make_minimal_dictionary(
                save_diagnostics=save_diagnostics,
                cpus=cpus,
                memory=memory,
                ephemeral_storage=ephemeral_storage,
            )
        )

    return attributes
