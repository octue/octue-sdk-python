import unittest

from octue.twined.cloud.events.attributes import QuestionAttributes
from octue.twined.cloud.events.question import make_question_event
from octue.twined.cloud.events.validation import raise_if_event_is_invalid

SENDER = "my-org/my-parent:1.0.0"
RECIPIENT = "my-org/my-child:2.0.0"


class TestMakeQuestionEvent(unittest.TestCase):
    def test_originator_question_attributes_are_serialised(self):
        """Test that the attributes of an originator question made without explicit attributes are in serialised form
        (e.g. the datetime is an ISO 8601 string rather than a `datetime` object).
        """
        question = make_question_event(input_values={"n": 5}, sender=SENDER, recipient=RECIPIENT)

        self.assertEqual(question["event"], {"kind": "question", "input_values": {"n": 5}})
        self.assertIsInstance(question["attributes"]["datetime"], str)
        self.assertEqual(question["attributes"]["sender"], SENDER)
        self.assertEqual(question["attributes"]["recipient"], RECIPIENT)

        # The question should be an originator question.
        self.assertEqual(question["attributes"]["question_uuid"], question["attributes"]["originator_question_uuid"])
        self.assertNotIn("parent_question_uuid", question["attributes"])

    def test_originator_question_is_valid_when_parsed_by_service(self):
        """Test that an originator question made without explicit attributes passes service communication schema
        validation when parsed in the same way as `Service._parse_question` (regression test for issue #744).
        """
        question = make_question_event(input_values={"n": 5}, sender=SENDER, recipient=RECIPIENT)
        attributes = QuestionAttributes.from_serialised_attributes(question["attributes"])

        # This raises `jsonschema.ValidationError` if the question is invalid.
        raise_if_event_is_invalid(event=question["event"], attributes=attributes, recipient=RECIPIENT)

        # Check the attributes round-trip to their deserialised types.
        self.assertEqual(attributes.retry_count, 0)
        self.assertIs(attributes.forward_logs, True)
