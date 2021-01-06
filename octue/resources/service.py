class Service:
    def __init__(self, name, id, uri):
        self.name = name
        self.id = id
        self.uri = uri

    def __repr(self):
        return f"<{type(self).__name__}({self.name!r})>"

    def ask(self, input_values, input_manifest=None):
        pass
