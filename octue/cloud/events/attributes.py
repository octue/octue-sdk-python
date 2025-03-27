import datetime as dt
import uuid as uuid_library

from octue.definitions import LOCAL_SDK_VERSION
from octue.utils.dictionaries import make_minimal_dictionary

PARENT_SENDER_TYPE = "PARENT"
CHILD_SENDER_TYPE = "CHILD"


class EventAttributes:
    """A data structure for holding and working with attributes for a single Octue Twined event. If originator and
    parent information aren't provided, the attributes will correspond to an event of any kind related to an originator
    question.

    :param str sender: the unique identifier (SRUID) of the service revision sending the question
    :param str recipient: the SRUID of the service revision the question is for
    :param str|None sender_type: the type of sender for this event; must be one of {"PARENT", "CHILD"}
    :param str|None uuid: the UUID of the event; if `None`, a UUID is generated
    :param datetime.datetime|None datetime: the datetime the event was created at; defaults to the current datetime in UTC
    :param str|None question_uuid: the UUID of the question; if `None`, a UUID is generated
    :param str|None parent_question_uuid: the UUID of the question that triggered this question; this should be `None` if this event relates to the first question in a question tree
    :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question; if `None`, the event's related question is assumed to be the originator question and `question_uuid` is used
    :param str|None parent: the SRUID of the service revision that asked the question this event is related to
    :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question; if `None`, the `sender` is used
    :param str sender_sdk_version: the semantic version of Octue SDK the sender is running; defaults to the version in the environment
    :param int retry_count: the retry count of the question this event is related to (this is zero if it's the first attempt at the question)
    :return None:
    """

    SENDER_TYPE = None

    def __init__(
        self,
        sender,
        recipient,
        sender_type=None,
        uuid=None,
        datetime=None,
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        parent=None,
        originator=None,
        sender_sdk_version=LOCAL_SDK_VERSION,
        retry_count=0,
    ):
        sender_type = sender_type or self.SENDER_TYPE
        self._validate_sender_type(sender_type)
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

    @classmethod
    def from_serialised_attributes(cls, serialised_attributes):
        """Extract a Twined service event's attributes and deserialise them to the expected form. This function doesn't
        assume the required attributes are present as validation hasn't happened yet.

        :param dict serialised_attributes: the event container in dictionary format or direct Google Pub/Sub format
        :return octue.cloud.events.attributes.EventAttributes: the extracted and deserialised attributes
        """
        serialised_attributes = dict(serialised_attributes)

        return cls(
            uuid=serialised_attributes.get("uuid"),
            datetime=serialised_attributes.get("datetime"),
            question_uuid=serialised_attributes.get("question_uuid"),
            parent_question_uuid=serialised_attributes.get("parent_question_uuid"),
            originator_question_uuid=serialised_attributes.get("originator_question_uuid"),
            sender=serialised_attributes.get("sender"),
            parent=serialised_attributes.get("parent"),
            originator=serialised_attributes.get("originator"),
            sender_type=serialised_attributes.get("sender_type"),
            sender_sdk_version=serialised_attributes.get("sender_sdk_version"),
            recipient=serialised_attributes.get("recipient"),
            retry_count=int(serialised_attributes.get("retry_count")),
        )

    def reset_uuid_and_datetime(self):
        """Set a new UUID and datetime. This avoids having to create a new instance for every single event (for which
        all other attributes are the same).

        :return None:
        """
        self.uuid = str(uuid_library.uuid4())
        self.datetime = dt.datetime.now(tz=dt.timezone.utc)

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

            serialised_attributes[key] = value

        return serialised_attributes

    def _validate_sender_type(self, sender_type):
        """Check that the given sender type is the same as the `SENDER_TYPE` attribute.

        :param str sender_type: the sender type to check
        :raise ValueError: if the given sender type is not the same as the `SENDER_TYPE` attribute
        :return None:
        """
        if sender_type != self.SENDER_TYPE:
            raise ValueError(f"`sender_type` must be {self.SENDER_TYPE!r} for {type(self).__name__!r}.")


class QuestionAttributes(EventAttributes):
    """A data structure for holding and working with attributes for a single question event. If originator and parent
    information aren't provided, the attributes will correspond to an originator question event.

    :param str sender: the unique identifier (SRUID) of the service revision sending the question
    :param str recipient: the SRUID of the service revision the question is for
    :param str|None sender_type: the type of sender for this event; if provided, it must be "PARENT"
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

    SENDER_TYPE = PARENT_SENDER_TYPE

    def __init__(
        self,
        sender,
        recipient,
        sender_type=None,
        uuid=None,
        datetime=None,
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        parent=None,
        originator=None,
        sender_sdk_version=LOCAL_SDK_VERSION,
        retry_count=0,
        forward_logs=True,
        save_diagnostics="SAVE_DIAGNOSTICS_ON_CRASH",
        cpus=None,
        memory=None,
        ephemeral_storage=None,
    ):
        super().__init__(
            sender=sender,
            recipient=recipient,
            sender_type=sender_type,
            uuid=uuid,
            datetime=datetime,
            question_uuid=question_uuid,
            parent_question_uuid=parent_question_uuid,
            originator_question_uuid=originator_question_uuid,
            parent=parent,
            originator=originator,
            sender_sdk_version=sender_sdk_version,
            retry_count=retry_count,
        )

        self.forward_logs = forward_logs
        self.save_diagnostics = save_diagnostics
        self.cpus = cpus
        self.memory = memory
        self.ephemeral_storage = ephemeral_storage

    @classmethod
    def from_serialised_attributes(cls, serialised_attributes):
        """Extract and deserialise the attributes specific to a question event. This function doesn't assume these
        attributes are present as validation hasn't happened yet.

        :param dict serialised_attributes: attributes for a question event
        :return octue.cloud.events.attributes.QuestionAttributes: the deserialised attributes
        """
        serialised_attributes = dict(serialised_attributes)
        cpus = serialised_attributes.get("cpus")

        if cpus:
            cpus = int(cpus)

        return cls(
            uuid=serialised_attributes.get("uuid"),
            datetime=serialised_attributes.get("datetime"),
            question_uuid=serialised_attributes.get("question_uuid"),
            parent_question_uuid=serialised_attributes.get("parent_question_uuid"),
            originator_question_uuid=serialised_attributes.get("originator_question_uuid"),
            sender=serialised_attributes.get("sender"),
            sender_type=serialised_attributes.get("sender_type"),
            parent=serialised_attributes.get("parent"),
            originator=serialised_attributes.get("originator"),
            sender_sdk_version=serialised_attributes.get("sender_sdk_version"),
            recipient=serialised_attributes.get("recipient"),
            retry_count=int(serialised_attributes.get("retry_count")),
            forward_logs=bool(int(serialised_attributes.get("forward_logs"))),
            save_diagnostics=serialised_attributes.get("save_diagnostics"),
            cpus=cpus,
            memory=serialised_attributes.get("memory"),
            ephemeral_storage=serialised_attributes.get("ephemeral_storage"),
        )

    def to_minimal_dict(self):
        """Convert the attributes to a minimal dictionary containing only the attributes that have a non-`None` value.
        Using a minimal dictionary means the smallest possible data structure is used so `None` values don't,
        for example, need to be redundantly encoded and transmitted when part of a JSON payload for a Pub/Sub message.

        :return dict: the non-`None` attributes
        """
        return {
            **super().to_minimal_dict(),
            **make_minimal_dictionary(
                forward_logs=self.forward_logs,
                save_diagnostics=self.save_diagnostics,
                cpus=self.cpus,
                memory=self.memory,
                ephemeral_storage=self.ephemeral_storage,
            ),
        }


class ResponseAttributes(EventAttributes):
    """A data structure for holding and working with attributes for a single response event of any kind. If originator
    and parent information aren't provided, the attributes will correspond to a response event related to an originator
    question.

    :param str sender: the unique identifier (SRUID) of the service revision sending the question
    :param str recipient: the SRUID of the service revision the question is for
    :param str|None sender_type: the type of sender for this event; if provided, it must be "CHILD"
    :param str|None uuid: the UUID of the event; if `None`, a UUID is generated
    :param datetime.datetime|None datetime: the datetime the event was created at; defaults to the current datetime in UTC
    :param str|None question_uuid: the UUID of the question; if `None`, a UUID is generated
    :param str|None parent_question_uuid: the UUID of the question that triggered this question; this should be `None` if this event relates to the first question in a question tree
    :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question; if `None`, the event's related question is assumed to be the originator question and `question_uuid` is used
    :param str|None parent: the SRUID of the service revision that asked the question this event is related to
    :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question; if `None`, the `sender` is used
    :param str sender_sdk_version: the semantic version of Octue SDK the sender is running; defaults to the version in the environment
    :param int retry_count: the retry count of the question this event is related to (this is zero if it's the first attempt at the question)
    :return None:
    """

    SENDER_TYPE = CHILD_SENDER_TYPE

    @classmethod
    def from_question_attributes(cls, question_attributes):
        """Create corresponding response attributes from a set of question attributes.

        :param octue.cloud.events.attributes.QuestionEventAttributes question_attributes: the question attributes to make the response attributes from
        :return octue.cloud.events.attributes.ResponseEventAttributes: the event attributes for a response event of any kind
        """
        return cls(
            sender=question_attributes.recipient,
            recipient=question_attributes.sender,
            question_uuid=question_attributes.question_uuid,
            parent_question_uuid=question_attributes.parent_question_uuid,
            originator_question_uuid=question_attributes.originator_question_uuid,
            parent=question_attributes.parent,
            originator=question_attributes.originator,
            sender_sdk_version=question_attributes.sender_sdk_version,
            retry_count=question_attributes.retry_count,
        )
