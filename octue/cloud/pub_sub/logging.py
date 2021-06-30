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
            self.publisher.publish(
                topic=self.topic.path,
                data=json.dumps({"log_record": vars(record)}).encode(),
                retry=create_custom_retry(self.timeout),
            )

        except RecursionError:
            raise
        except Exception:  # noqa
            self.handleError(record)
