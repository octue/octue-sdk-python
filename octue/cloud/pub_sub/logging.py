import logging
import re


ANSI_ESCAPE_SEQUENCES_PATTERN = r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"


class GooglePubSubHandler(logging.Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param callable message_sender: the `_send_message` method of the service that instantiated this instance
    :param octue.cloud.pub_sub.topic.Topic topic: topic to publish log records to
    :param str analysis_id: the UUID of the analysis the instance is handling the log records for
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(self, message_sender, topic, analysis_id, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.analysis_id = analysis_id
        self.timeout = timeout
        self._send_message = message_sender

    def emit(self, record):
        """Serialise the log record as a dictionary and publish it to the topic.

        :param logging.LogRecord record:
        :return None:
        """
        try:
            self._send_message(
                {
                    "type": "log_record",
                    "log_record": self._convert_log_record_to_primitives(record),
                    "analysis_id": self.analysis_id,
                    "message_number": self.topic.messages_published,
                },
                topic=self.topic,
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
        return serialised_record
