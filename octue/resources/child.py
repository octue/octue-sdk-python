from octue import exceptions
from octue.resources import service_backend
from octue.resources.service import Service


BACKEND_TO_SERVICE_MAPPING = {service_backend.GCPPubSubBackend: Service}


class Child:
    """ A class representing a child service that can be asked questions. """

    def __init__(self, name, id, backend):
        self.name = name
        self.id = id

        backend = self._get_backend(backend.pop("name"))(**backend)
        self._service = BACKEND_TO_SERVICE_MAPPING[type(backend)](name=f"{self.name}-local", backend=backend)

    def ask(self, input_values, input_manifest=None, timeout=20):
        """ Ask the child a question (i.e. send it some input value and/or a manifest and wait for it to run an analysis
        on them and return the output values). The input values given must adhere to the Twine file of the child.
        """
        subscription = self._service.ask(self.id, input_values, input_manifest)
        return self._service.wait_for_answer(subscription, timeout)

    def _get_backend(self, backend_name):
        available_backends = {key: value for key, value in vars(service_backend) if key.endswith("Backend")}

        if backend_name not in available_backends:
            raise exceptions.BackendNotFound(
                f"Backend with name {backend_name} not found. Available backends are {list(available_backends.keys())}"
            )

        return available_backends[backend_name]
