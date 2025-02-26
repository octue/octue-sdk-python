import datetime as dt
import json
import uuid as uuid_library

from octue.definitions import LOCAL_SDK_VERSION
from octue.utils.dictionaries import make_minimal_dictionary

SENDER_TYPE_OPPOSITES = {"CHILD": "PARENT", "PARENT": "CHILD"}


class EventAttributes:
    def __init__(
        self,
        sender,
        sender_type,
        recipient,
        uuid=None,
        datetime=None,
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        parent=None,
        originator=None,
        sender_sdk_version=LOCAL_SDK_VERSION,
        retry_count=0,
        forward_logs=None,
        save_diagnostics=None,
        cpus=None,
        memory=None,
        ephemeral_storage=None,
    ):
        # Attributes for all event kinds.
        self.uuid = uuid or str(uuid_library.uuid4())
        self.datetime = datetime or dt.datetime.now(tz=dt.timezone.utc).isoformat()
        self.question_uuid = question_uuid or str(uuid_library.uuid4())
        self.parent_question_uuid = parent_question_uuid
        self.originator_question_uuid = originator_question_uuid or self.question_uuid
        self.sender = sender
        self.parent = parent or self.sender
        self.originator = originator or self.sender
        self.sender_type = sender_type
        self.sender_sdk_version = sender_sdk_version
        self.recipient = recipient
        self.retry_count = retry_count

        # Question event attributes.
        self.forward_logs = forward_logs
        self.save_diagnostics = save_diagnostics
        self.cpus = cpus
        self.memory = memory
        self.ephemeral_storage = ephemeral_storage

    def make_response_attributes(self):
        attributes = self.to_dict()
        attributes["sender"] = self.recipient
        attributes["sender_type"] = SENDER_TYPE_OPPOSITES[self.sender_type]
        attributes["sender_sdk_version"] = LOCAL_SDK_VERSION
        attributes["recipient"] = self.sender

        for attr in ("forward_logs", "save_diagnostics", "cpus", "memory", "ephemeral_storage"):
            if attr in attributes:
                del attributes[attr]

        return EventAttributes(**attributes)

    def to_dict(self):
        return make_minimal_dictionary(
            uuid=self.uuid,
            datetime=self.datetime,
            question_uuid=self.question_uuid,
            parent_question_uuid=self.parent_question_uuid,
            originator_question_uuid=self.originator_question_uuid,
            parent=self.parent,
            originator=self.originator,
            sender=self.sender,
            sender_type=self.sender_type,
            sender_sdk_version=self.sender_sdk_version,
            recipient=self.recipient,
            retry_count=self.retry_count,
            forward_logs=self.forward_logs,
            save_diagnostics=self.save_diagnostics,
            cpus=self.cpus,
            memory=self.memory,
            ephemeral_storage=self.ephemeral_storage,
        )

    def to_serialised_attributes(self):
        serialised_attributes = {}

        for key, value in self.to_dict().items():
            if isinstance(value, bool):
                value = str(int(value))
            elif isinstance(value, (int, float)):
                value = str(value)
            elif value is None:
                value = json.dumps(value)

            serialised_attributes[key] = value

        return serialised_attributes
