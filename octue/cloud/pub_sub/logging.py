import logging
import re

ANSI_ESCAPE_SEQUENCES_PATTERN = r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"


class GoogleCloudPubSubHandler(logging.Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param callable event_emitter: the `_emit_event` method of the service that instantiated this instance
    :param octue.cloud.events.attributes.ResponseAttributes attributes: the attributes to use for the log record event
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(self, event_emitter, attributes, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attributes = attributes
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
                attributes=self.attributes,
                wait=False,
            )

        except Exception:  # noqa
            self.handleError(record)

    @staticmethod
    def _convert_log_record_to_primitives(log_record):
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
