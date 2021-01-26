import logging
import google.api_core.exceptions


logger = logging.getLogger(__name__)


class Topic:
    """ A candidate topic to use with Google Pub/Sub. The topic represented by an instance of this class does not
    necessarily already exist on the Google Pub/Sub servers.
    """

    def __init__(self, name, namespace, service):
        self.name = name
        self.service = service
        self.path = self.service._publisher.topic_path(service.backend.project_name, f"{namespace}.{self.name}")

    def create(self, allow_existing=False):
        """ Create a Google Pub/Sub topic that can be published to. """
        if not allow_existing:
            self.service._publisher.create_topic(name=self.path)
            self._log_creation()
            return

        try:
            self.service._publisher.create_topic(name=self.path)
        except google.api_core.exceptions.AlreadyExists:
            pass
        self._log_creation()

    def delete(self):
        """ Delete the topic from Google Pub/Sub. """
        self.service._publisher.delete_topic(topic=self.path)
        logger.debug("%r deleted topic %r.", self.service, self.path)

    def exists(self):
        """ Check if the topic exists on the Google Pub/Sub servers. """
        try:
            self.service._publisher.get_topic(topic=self.path)
        except google.api_core.exceptions.NotFound:
            return False
        return True

    def _log_creation(self):
        """ Log the creation of the topic. """
        logger.debug("%r created topic %r.", self.service, self.path)
