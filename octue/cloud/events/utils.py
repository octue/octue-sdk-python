import datetime
import uuid

from octue.cloud import LOCAL_SDK_VERSION


def make_originator_question_event(input_values, input_manifest, parent_sruid, child_sruid, question_uuid=None):
    question_uuid = question_uuid or uuid.uuid4()

    return {
        "event": {
            "input_values": input_values,
            "input_manifest": input_manifest,
        },
        "attributes": make_attributes(
            question_uuid=question_uuid,
            parent_question_uuid=question_uuid,
            originator_question_uuid=question_uuid,
            parent=parent_sruid,
            originator=parent_sruid,
            sender=parent_sruid,
            recipient=child_sruid,
        ),
    }


def make_attributes(
    parent_question_uuid,
    originator_question_uuid,
    parent,
    originator,
    sender,
    recipient,
    question_uuid=None,
    retry_count=0,
):
    return {
        "uuid": str(uuid.uuid4()),
        "datetime": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "question_uuid": question_uuid or str(uuid.uuid4()),
        "parent_question_uuid": parent_question_uuid,
        "originator_question_uuid": originator_question_uuid,
        "parent": parent,
        "originator": originator,
        "sender": sender,
        "sender_sdk_version": LOCAL_SDK_VERSION,
        "recipient": recipient,
        "retry_count": retry_count,
    }
