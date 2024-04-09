import logging
import re


ANSI_ESCAPE_SEQUENCES_PATTERN = r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"


class GoogleCloudPubSubHandler(logging.Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param callable event_emitter: the `_emit_event` method of the service that instantiated this instance
    :param str question_uuid: the UUID of the question to handle log records for
    :param str originator: the SRUID of the service that asked the question these log records are related to
    :param str recipient: the SRUID of the service to send these log records to
    :param octue.cloud.events.counter.EventCounter order: an event counter keeping track of the order of emitted events
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(self, event_emitter, question_uuid, originator, recipient, order, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.question_uuid = question_uuid
        self.originator = originator
        self.recipient = recipient
        self.order = order
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
                originator=self.originator,
                recipient=self.recipient,
                order=self.order,
                attributes={
                    "question_uuid": self.question_uuid,
                    "sender_type": "CHILD",  # The sender type is repeated here as a string to avoid a circular import.
                },
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
