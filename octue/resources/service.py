import logging
import socketio


logger = logging.getLogger(__name__)


class Service:
    def __init__(self, name, id_, uri):
        self.name = name
        self.id = id_
        self.uri = uri
        self.client = socketio.Client()
        self.response = None

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self, namespaces=None):
        namespaces = namespaces or ["/octue"]

        try:
            self.client.connect(self.uri, namespaces=namespaces)
            logger.info("%r service connected to server at %s using namespaces %s", self.name, self.uri, namespaces)
        except socketio.exceptions.ConnectionError as error:
            logger.error(
                "%r service failed to connect to server at %s using namespaces %s", self.name, self.uri, namespaces
            )
            raise error

    def disconnect(self):
        self.client.disconnect()
        logger.info("%r service disconnected.", self.name)

    async def ask(self, input_values, input_manifest=None):
        self.client.emit(event="question", data=input_values, callback=self._question_callback, namespace="/octue")
        response = self.response
        self.response = None
        return response

    def _question_callback(self, item):
        self.response = item


class MockClient:
    def connect(self, uri, namespaces):
        pass

    def emit(self, event, data, callback, namespace):
        """ Return the data as it was provided. """
        callback(data)
