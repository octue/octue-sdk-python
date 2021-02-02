import logging
import google.api_core.exceptions


logger = logging.getLogger(__name__)


class Subscription:
    """A candidate subscription to use with Google Pub/Sub. The subscription represented by an instance of this class
    does not necessarily already exist on the Google Pub/Sub servers.
    """

    def __init__(self, name, topic, namespace, service):
        self.name = name
        self.topic = topic
        self.service = service
        self.path = self.service.subscriber.subscription_path(
            self.service.backend.project_name, f"{namespace}.{self.name}"
        )

    def create(self, allow_existing=False):
        """ Create a Google Pub/Sub subscription that can be subscribed to. """
        if not allow_existing:
            self.service.subscriber.create_subscription(topic=self.topic.path, name=self.path)
            self._log_creation()
            return

        try:
            self.service.subscriber.create_subscription(topic=self.topic.path, name=self.path)
        except google.api_core.exceptions.AlreadyExists:
            pass
        self._log_creation()

    def delete(self):
        """ Delete the subscription from Google Pub/Sub. """
        self.service.subscriber.delete_subscription(subscription=self.path)
        logger.debug("%r deleted subscription %r.", self.service, self.path)

    def _log_creation(self):
        """ Log the creation of the subscription. """
        logger.debug("%r created subscription %r.", self.service, self.path)
