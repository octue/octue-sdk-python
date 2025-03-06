import unittest

from octue.cloud.events.attributes import EventAttributes

QUESTION_UUID = "50760303-ee89-4752-81cc-aadd05f81752"
SENDER = "my-org/my-parent:1.0.0"
SENDER_TYPE = "PARENT"
RECIPIENT = "my-org/my-child:2.0.0"


class TestEventAttributes(unittest.TestCase):
    def test_defaults(self):
        """Test that the defaults are correct."""
        attributes = EventAttributes(
            sender=SENDER,
            sender_type=SENDER_TYPE,
            recipient=RECIPIENT,
            question_uuid=QUESTION_UUID,
        )

        attributes_dict = attributes.__dict__
        self.assertTrue(attributes_dict.pop("uuid"))
        self.assertTrue(attributes_dict.pop("datetime"))
        self.assertTrue(attributes_dict.pop("sender_sdk_version"))

        self.assertEqual(
            attributes_dict,
            {
                "sender": SENDER,
                "sender_type": SENDER_TYPE,
                "recipient": RECIPIENT,
                "question_uuid": QUESTION_UUID,
                "parent_question_uuid": None,
                "originator_question_uuid": QUESTION_UUID,
                "parent": SENDER,
                "originator": SENDER,
                "retry_count": 0,
                "forward_logs": None,
                "save_diagnostics": None,
                "cpus": None,
                "memory": None,
                "ephemeral_storage": None,
            },
        )

    def test_refresh(self):
        """Test that refreshing the attributes changes the UUID and datetime."""
        attributes = EventAttributes(
            sender=SENDER,
            sender_type=SENDER_TYPE,
            recipient=RECIPIENT,
            question_uuid=QUESTION_UUID,
        )

        original_uuid = attributes.uuid
        original_datetime = attributes.datetime

        attributes.refresh()
        self.assertNotEqual(attributes.uuid, original_uuid)
        self.assertNotEqual(attributes.datetime, original_datetime)

    def test_make_opposite_attributes(self):
        """Test that the sender and recipient are reversed when making opposite attributes from a set of attributes."""
        attributes = EventAttributes(
            sender=SENDER,
            sender_type=SENDER_TYPE,
            recipient=RECIPIENT,
            question_uuid=QUESTION_UUID,
        )

        opposite_attributes = attributes.make_opposite_attributes()

        opposite_attributes_dict = opposite_attributes.__dict__
        self.assertTrue(opposite_attributes_dict.pop("uuid"))
        self.assertTrue(opposite_attributes_dict.pop("datetime"))
        self.assertTrue(opposite_attributes_dict.pop("sender_sdk_version"))

        self.assertEqual(
            opposite_attributes_dict,
            {
                "sender": RECIPIENT,
                "sender_type": "CHILD",
                "recipient": SENDER,
                "question_uuid": QUESTION_UUID,
                "parent_question_uuid": None,
                "originator_question_uuid": QUESTION_UUID,
                "parent": SENDER,
                "originator": SENDER,
                "retry_count": 0,
                "forward_logs": None,
                "save_diagnostics": None,
                "cpus": None,
                "memory": None,
                "ephemeral_storage": None,
            },
        )

    def test_to_minimal_dict(self):
        """Test that non-`None` attributes are excluded when making a minimal dictionary from attributes."""
        attributes = EventAttributes(
            sender=SENDER,
            sender_type=SENDER_TYPE,
            recipient=RECIPIENT,
            question_uuid=QUESTION_UUID,
        )

        attributes_dict = attributes.to_minimal_dict()
        self.assertTrue(attributes_dict.pop("uuid"))
        self.assertTrue(attributes_dict.pop("datetime"))
        self.assertTrue(attributes_dict.pop("sender_sdk_version"))

        self.assertEqual(
            attributes_dict,
            {
                "sender": SENDER,
                "sender_type": SENDER_TYPE,
                "recipient": RECIPIENT,
                "question_uuid": QUESTION_UUID,
                "originator_question_uuid": QUESTION_UUID,
                "parent": SENDER,
                "originator": SENDER,
                "retry_count": 0,
            },
        )

    def test_to_serialised_attributes(self):
        """Test that attributes are serialised correctly."""
        attributes = EventAttributes(
            sender=SENDER,
            sender_type=SENDER_TYPE,
            recipient=RECIPIENT,
            question_uuid=QUESTION_UUID,
            forward_logs=True,
            save_diagnostics="SAVE_DIAGNOSTICS_ON",
            cpus=1,
            memory="2Gi",
            ephemeral_storage="256Mi",
        )

        serialised_attributes = attributes.to_serialised_attributes()

        self.assertTrue(serialised_attributes.pop("uuid"))
        self.assertTrue(serialised_attributes.pop("sender_sdk_version"))
        self.assertTrue(isinstance(serialised_attributes.pop("datetime"), str))

        self.assertEqual(
            serialised_attributes,
            {
                "sender": SENDER,
                "sender_type": SENDER_TYPE,
                "recipient": RECIPIENT,
                "question_uuid": QUESTION_UUID,
                "originator_question_uuid": QUESTION_UUID,
                "parent": SENDER,
                "originator": SENDER,
                "retry_count": "0",
                "forward_logs": "1",
                "save_diagnostics": "SAVE_DIAGNOSTICS_ON",
                "cpus": "1",
                "memory": "2Gi",
                "ephemeral_storage": "256Mi",
            },
        )
