from octue.cloud.events.attributes import QuestionAttributes
from octue.utils.dictionaries import make_minimal_dictionary


def make_question_event(
    input_values=None,
    input_manifest=None,
    sender=None,
    recipient=None,
    question_uuid=None,
    attributes=None,
):
    """Make a question event. If the `attributes` argument isn't provided, the question will be an originator question.

    :param dict|None input_values: any input values for the question
    :param dict|None input_manifest: an input manifest of any datasets needed for the question as a python primitive
    :param str|None sender: the service revision unique identifier (SRUID) of the service revision sending the question
    :param str|None recipient: the service revision unique identifier (SRUID) of the service revision the question is for
    :param str|None question_uuid: the UUID to use for the question; if `None`, a UUID is generated
    :param dict|None attributes: the attributes to use for the question event; if none are provided, the question will be an originator question
    :return dict: the question event and its attributes
    """
    if not attributes:
        attributes = QuestionAttributes(
            question_uuid=question_uuid,
            sender=sender,
            recipient=recipient,
        ).to_minimal_dict()

    return {
        "event": make_minimal_dictionary(input_values=input_values, input_manifest=input_manifest, kind="question"),
        "attributes": attributes,
    }
