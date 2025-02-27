import datetime as dt
import json
import uuid as uuid_library

from octue.definitions import LOCAL_SDK_VERSION
from octue.utils.dictionaries import make_minimal_dictionary

SENDER_TYPE_OPPOSITES = {"CHILD": "PARENT", "PARENT": "CHILD"}


class EventAttributes:
    """A data structure for holding and working with attributes for a single Octue Twined event.

    :param str sender: the unique identifier (SRUID) of the service revision sending the question
    :param str sender_type: the type of sender for this event; must be one of {"PARENT", "CHILD"}
    :param str recipient: the SRUID of the service revision the question is for
    :param str|None uuid: the UUID of the event; if `None`, a UUID is generated
    :param datetime.datetime|None datetime: the datetime the event was created at; defaults to the current datetime in UTC
    :param str|None question_uuid: the UUID of the question; if `None`, a UUID is generated
    :param str|None parent_question_uuid: the UUID of the question that triggered this question; this should be `None` if this event relates to the first question in a question tree
    :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question; if `None`, the event's related question is assumed to be the originator question and `question_uuid` is used
    :param str|None parent: the SRUID of the service revision that asked the question this event is related to
    :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question; if `None`, the `sender` is used
    :param str sender_sdk_version: the semantic version of Octue SDK the sender is running; defaults to the version in the environment
    :param int retry_count: the retry count of the question this event is related to (this is zero if it's the first attempt at the question)
    :param bool|None forward_logs: if this isn't a `question` event, this should be `None`; otherwise, it should be a boolean indicating whether the parent requested the child to forward its logs to it
    :param str|None save_diagnostics: if this isn't a `question` event, this should be `None`; otherwise, it must be one of {"SAVE_DIAGNOSTICS_OFF", "SAVE_DIAGNOSTICS_ON_CRASH", "SAVE_DIAGNOSTICS_ON"}
    :param int|None cpus: if this isn't a `question` event, this should be `None`; otherwise, it can be `None` or the number of CPUs requested for the question
    :param str|None memory: if this isn't a `question` event, this should be `None`; otherwise, it can be `None` or the amount of memory requested for the question e.g. "256Mi" or "1Gi"
    :param str|None ephemeral_storage: if this isn't a `question` event, this should be `None`; otherwise, it can be `None` or the amount of ephemeral storage requested for the question e.g. "256Mi" or "1Gi"
    :return None:
    """

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
        self.datetime = datetime or dt.datetime.now(tz=dt.timezone.utc)
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

    def make_opposite_attributes(self):
        """Create the attributes for an event of the opposite sender type to this event (parent -> child or child
        -> parent). For example, if these attributes are for a question event, create the attributes for a response
        event such as a result or log record.

        :return octue.cloud.events.attributes.EventAttributes: the event attributes for an event with the opposite sender type
        """
        attributes = self.to_minimal_dict()
        attributes["sender"] = self.recipient
        attributes["recipient"] = self.sender
        attributes["sender_type"] = SENDER_TYPE_OPPOSITES[self.sender_type]
        attributes["sender_sdk_version"] = LOCAL_SDK_VERSION

        for attr in ("forward_logs", "save_diagnostics", "cpus", "memory", "ephemeral_storage"):
            if attr in attributes:
                del attributes[attr]

        return EventAttributes(**attributes)

    def to_minimal_dict(self):
        """Convert the attributes to a minimal dictionary containing only the attributes that have a non-`None` value.
        Using a minimal dictionary means the smallest possible data structure is used so `None` values don't,
        for example, need to be redundantly encoded and transmitted when part of a JSON payload for a Pub/Sub message.

        :return dict: the non-`None` attributes
        """
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
        """Convert the attributes to their serialised forms. This is required for e.g. sending the attributes as message
        attributes on a Pub/Sub message. A minimal dictionary is produced containing only the attributes that have a
        non-`None` value.

        :return dict: the attribute names of the non-`None` attributes mapped to their serialised values
        """
        serialised_attributes = {}

        for key, value in self.to_minimal_dict().items():
            if isinstance(value, bool):
                value = str(int(value))
            elif isinstance(value, (int, float)):
                value = str(value)
            elif isinstance(value, dt.datetime):
                value = value.isoformat()
            elif value is None:
                value = json.dumps(value)

            serialised_attributes[key] = value

        return serialised_attributes
