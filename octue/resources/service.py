import socketio


class Service:
    def __init__(self, name, id, uri):
        self.name = name
        self.id = id
        self.client = self._create_socketio_client(uri)
        self.response = None

    def __repr__(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def ask(self, input_values, input_manifest=None):
        self.client.emit(event="question", data=input_values, callback=self._question_callback, namespace="/octue")
        return self.response

    def _create_socketio_client(self, uri):
        client = socketio.Client()
        client.connect(uri, namespaces=["/octue"])
        return client

    def _question_callback(self, item):
        self.response = item
