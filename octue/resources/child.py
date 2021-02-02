from octue.resources.communication import service_backends
from octue.resources.communication.google_pub_sub.service import Service


BACKEND_TO_SERVICE_MAPPING = {service_backends.GCPPubSubBackend: Service}


class Child:
    """A class representing a child service that can be asked questions. It is a convenience wrapper for `Service` that
    makes the asking of questions more intuitive for Scientists and allows easier selection of backends.
    """

    def __init__(self, name, id, backend):
        self.name = name
        self.id = id

        backend = service_backends.get_backend(backend.pop("name"))(**backend)
        self._service = BACKEND_TO_SERVICE_MAPPING[type(backend)](backend=backend)

    def ask(self, input_values, input_manifest=None, timeout=20):
        """Ask the child a question (i.e. send it some input value and/or a manifest and wait for it to run an analysis
        on them and return the output values). The input values given must adhere to the Twine file of the child.
        """
        subscription = self._service.ask(self.id, input_values, input_manifest)
        return self._service.wait_for_answer(subscription, timeout)
