import datetime
import uuid

from octue.cloud import LOCAL_SDK_VERSION
from octue.utils.dictionaries import make_minimal_dictionary


def make_question_event(
    input_values,
    input_manifest,
    parent_sruid=None,
    child_sruid=None,
    question_uuid=None,
    attributes=None,
):
    """Make a question event. If the `attributes` argument isn't provided, the question will be an originator question.

    :param dict input_values:
    :param octue.resources.manifest.Manifest input_manifest:
    :param str parent_sruid:
    :param str child_sruid:
    :param str question_uuid:
    :param dict attributes:
    :return dict:
    """
    if not attributes:
        question_uuid = question_uuid or str(uuid.uuid4())

        attributes = make_attributes(
            question_uuid=question_uuid,
            parent_question_uuid=question_uuid,
            originator_question_uuid=question_uuid,
            parent=parent_sruid,
            originator=parent_sruid,
            sender=parent_sruid,
            recipient=child_sruid,
            forward_logs=True,
            save_diagnostics="SAVE_DIAGNOSTICS_ON",
            sender_type="PARENT",
        )

    return {
        "event": make_minimal_dictionary(input_values=input_values, input_manifest=input_manifest, kind="question"),
        "attributes": attributes,
    }


def make_attributes(
    parent_question_uuid,
    originator_question_uuid,
    parent,
    originator,
    sender,
    sender_type,
    recipient,
    question_uuid=None,
    retry_count=0,
    forward_logs=None,
    save_diagnostics=None,
):
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
        "retry_count": retry_count,
    }

    if sender_type == "PARENT":
        if forward_logs is None or save_diagnostics is None:
            raise ValueError(
                "`forward_logs` and `save_diagnostics` must be present in the attributes if the sender type is "
                "'PARENT'."
            )

        attributes["forward_logs"] = forward_logs
        attributes["save_diagnostics"] = save_diagnostics

    return attributes
