import logging
import time
from datetime import datetime

import google.api_core.exceptions
from google.pubsub_v1.types.pubsub import Topic as Topic_


logger = logging.getLogger(__name__)


class Topic:
    """A candidate topic to use with Google Pub/Sub. The topic represented by an instance of this class does not
    necessarily already exist on the Google Pub/Sub servers.

    :param str name:
    :param str namespace:
    :param octue.cloud.pub_sub.service.Service service:
    :return None:
    """

    def __init__(self, name, service, namespace=""):
        if namespace and not name.startswith(namespace):
            self.name = f"{namespace}.{name}"
        else:
            self.name = name

        self.service = service
        self.path = self.generate_topic_path(service.backend.project_name, self.name)
        self.messages_published = 0

    def __repr__(self):
        """Represent the topic as a string.

        :return str:
        """
        return f"<{type(self).__name__}({self.name})>"

    def create(self, allow_existing=False):
        """Create a Google Pub/Sub topic that can be published to.

        :param bool allow_existing: if `False`, raise an error if a topic of this name already exists; if `True`, do nothing (the existing topic is not overwritten)
        :return None:
        """
        posix_timestamp_with_no_decimals = str(datetime.now().timestamp()).split(".")[0]

        if not allow_existing:
            self.service.publisher.create_topic(
                request=Topic_(name=self.path, labels={"created": posix_timestamp_with_no_decimals})
            )

            self._log_creation()
            return

        try:
            self.service.publisher.create_topic(
                request=Topic_(name=self.path, labels={"created": posix_timestamp_with_no_decimals})
            )
        except google.api_core.exceptions.AlreadyExists:
            pass
        self._log_creation()

    def get_subscriptions(self):
        """Get the topic's subscriptions' paths.

        :return list(str):
        """
        return list(self.service.publisher.list_topic_subscriptions(topic=self.path))

    def delete(self):
        """Delete the topic from Google Pub/Sub.

        :return None:
        """
        self.service.publisher.delete_topic(topic=self.path)
        logger.debug("%r deleted topic %r.", self.service, self.path)

    def exists(self, timeout=10):
        """Check if the topic exists on the Google Pub/Sub servers.

        :param float timeout:
        :return bool:
        """
        start_time = time.time()

        while time.time() - start_time <= timeout:
            try:
                self.service.publisher.get_topic(topic=self.path)
                return True
            except google.api_core.exceptions.NotFound:
                time.sleep(1)

        return False

    @staticmethod
    def generate_topic_path(project_name, topic_name):
        """Generate a full topic path in the format `projects/<project_name>/topics/<topic_name>`.

        :param str project_name:
        :param str topic_name:
        :return str:
        """
        return f"projects/{project_name}/topics/{topic_name}"

    def _log_creation(self):
        """Log the creation of the topic.

        :return None:
        """
        logger.debug("%r created topic %r.", self.service, self.path)
