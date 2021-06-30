import json
from logging import Handler

from octue.cloud.pub_sub import create_custom_retry


class GooglePubSubHandler(Handler):
    def __init__(self, publisher, topic, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.timeout = timeout
        self.publisher = publisher

    def emit(self, record):
        try:
            message = self.format(record)

            self.publisher.publish(
                topic=self.topic.path,
                data=json.dumps({"log_message": message, "log_level": record.levelname.lower()}).encode(),
                retry=create_custom_retry(self.timeout),
            )

        except RecursionError:  # See issue 36272
            raise
        except Exception:  # noqa
            self.handleError(record)
