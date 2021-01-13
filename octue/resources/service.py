import logging
import socketio


logger = logging.getLogger(__name__)


class Service:
    def __init__(self, name, id, uri):
        self.name = name
        self.id = id
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

    def connect(self):
        self.client.connect(self.uri)
        logger.info("%r connected to server at %s.", self, self.uri)

        # This is an awful hack.
        if "/" not in self.client.namespaces:
            self.client.namespaces["/"] = None

    def disconnect(self):
        self.client.disconnect()
        logger.info("%r disconnected.", self)

    # def ask(self, input_values, input_manifest=None):
    #     logger.debug("Asking question")
    #     self.client.emit("question", input_values, callback=self._question_callback)
    #     response = self.response
    #     print(response)
    #     self.response = None
    #     return response

    def _question_callback(self, item):
        self.response = item


class MockClient:
    def connect(self, uri, environ):
        pass

    def disconnect(self, session_id):
        pass

    def emit(self, event, data, callback):
        """ Return the data as it was provided. """
        callback(data)
