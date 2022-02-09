import json
import logging

from google.api_core import retry


class GooglePubSubHandler(logging.Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param google.cloud.pubsub_v1.PublisherClient publisher: pub/sub publisher to use to publish the log records
    :param octue.cloud.pub_sub.topic.Topic topic: topic to publish log records to
    :param str analysis_id: the UUID of the analysis the instance is handling the log records for
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(self, publisher, topic, analysis_id, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.analysis_id = analysis_id
        self.timeout = timeout
        self._publisher = publisher

    def emit(self, record):
        """Serialise the log record as a dictionary and publish it to the topic.

        :param logging.LogRecord record:
        :return None:
        """
        try:
            if record.levelno == logging.ERROR:
                record.exc_info = tuple()

            self._publisher.publish(
                topic=self.topic.path,
                data=json.dumps(
                    {
                        "type": "log_record",
                        "log_record": vars(record),
                        "analysis_id": self.analysis_id,
                        "message_number": self.topic.messages_published,
                    }
                ).encode(),
                retry=retry.Retry(deadline=self.timeout),
            )

            self.topic.messages_published += 1

        except Exception:  # noqa
            self.handleError(record)
