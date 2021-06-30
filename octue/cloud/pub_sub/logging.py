import json
from logging import Handler

from octue.cloud.pub_sub import create_custom_retry


class GooglePubSubHandler(Handler):
    """A log handler that publishes log records to a Google Cloud Pub/Sub topic.

    :param google.cloud.pubsub_v1.PublisherClient publisher: pub/sub publisher to use to publish the log records
    :param octue.cloud.pub_sub.topic.Topic topic: topic to publish log records to
    :param float timeout: timeout in seconds for attempting to publish each log record
    :return None:
    """

    def __init__(self, publisher, topic, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.timeout = timeout
        self.publisher = publisher

    def emit(self, record):
        """Serialise the log record as a dictionary and publish it to the topic.

        :param logging.LogRecord record:
        :return None:
        """
        try:
            self.publisher.publish(
                topic=self.topic.path,
                data=json.dumps({"log_record": vars(record)}).encode(),
                retry=create_custom_retry(self.timeout),
            )

        except Exception:  # noqa
            self.handleError(record)
