import uuid

from octue.cloud.events.attributes import EventAttributes
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

        attributes = EventAttributes(
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
        "attributes": attributes.to_dict(),
    }
