import logging
import socketio


logger = logging.getLogger(__name__)


class Service:
    def __init__(self, name, id, uri):
        self.name = name
        self.id = id
        self.client = self._create_socketio_client(uri)
        self.response = None

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    async def ask(self, input_values, input_manifest=None):
        self.client.emit(event="question", data=input_values, callback=self._question_callback, namespace="/octue")
        response = self.response
        self.response = None
        return response

    def _create_socketio_client(self, uri):
        client = socketio.Client()
        namespaces = ["/octue"]

        try:
            client.connect(uri, namespaces=namespaces)
            logger.info("Connected to server at %s using namespaces %s", uri, namespaces)
        except socketio.exceptions.ConnectionError as error:
            logger.error("Failed to connect to server at %s using namespaces %s", uri, namespaces)
            raise error

        return client

    def _question_callback(self, item):
        self.response = item


class MockClient:
    def connect(self, uri, namespaces):
        pass

    def emit(self, event, data, callback, namespace):
        """ Return the data as it was provided. """
        callback(data)
