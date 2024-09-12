import logging
import re


ANSI_ESCAPE_SEQUENCES_PATTERN = r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"


class GoogleCloudPubSubHandler(logging.Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param callable event_emitter: the `_emit_event` method of the service that instantiated this instance
    :param str question_uuid: the UUID of the question to handle log records for
    :param str|None parent_question_uuid: the UUID of the question these log records are related to
    :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question
    :param str parent: the SRUID of the parent that asked the question these log records are related to
    :param str originator: the SRUID of the service revision that triggered the tree of questions these log records are related to
    :param str recipient: the SRUID of the service to send these log records to
    :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(
        self,
        event_emitter,
        question_uuid,
        parent_question_uuid,
        originator_question_uuid,
        parent,
        originator,
        recipient,
        retry_count,
        timeout=60,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.question_uuid = question_uuid
        self.parent_question_uuid = parent_question_uuid
        self.originator_question_uuid = originator_question_uuid
        self.parent = parent
        self.originator = originator
        self.recipient = recipient
        self.retry_count = retry_count
        self.timeout = timeout
        self._emit_event = event_emitter

    def emit(self, record):
        """Serialise the log record as a dictionary and publish it to the topic.

        :param logging.LogRecord record:
        :return None:
        """
        try:
            self._emit_event(
                {
                    "kind": "log_record",
                    "log_record": self._convert_log_record_to_primitives(record),
                },
                parent=self.parent,
                originator=self.originator,
                recipient=self.recipient,
                retry_count=self.retry_count,
                question_uuid=self.question_uuid,
                parent_question_uuid=self.parent_question_uuid,
                originator_question_uuid=self.originator_question_uuid,
                # The sender type is repeated here as a string to avoid a circular import.
                attributes={"sender_type": "CHILD"},
            )

        except Exception:  # noqa
            self.handleError(record)

    def _convert_log_record_to_primitives(self, log_record):
        """Convert a log record to JSON-serialisable primitives by interpolating the args into the message, and
        removing the exception info, which is potentially not JSON-serialisable. This is similar to the approach in
        `logging.handlers.SocketHandler.makePickle`. Also strip any ANSI escape sequences from the message.

        :param logging.LogRecord log_record:
        :return dict:
        """
        serialised_record = vars(log_record)

        serialised_record["msg"] = re.compile(ANSI_ESCAPE_SEQUENCES_PATTERN).sub("", log_record.getMessage())
        serialised_record["args"] = None
        serialised_record["exc_info"] = None
        serialised_record.pop("message", None)

        if not serialised_record.get("levelno"):
            serialised_record["levelno"] = logging.INFO

        return serialised_record
