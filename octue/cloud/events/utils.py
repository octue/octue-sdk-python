from octue.cloud.events.attributes import EventAttributes
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
        attributes = EventAttributes(
            question_uuid=question_uuid,
            sender=sender,
            recipient=recipient,
            forward_logs=True,
            save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",
            sender_type="PARENT",
        ).to_dict()

    return {
        "event": make_minimal_dictionary(input_values=input_values, input_manifest=input_manifest, kind="question"),
        "attributes": attributes,
    }
