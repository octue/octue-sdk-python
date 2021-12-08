from octue.cloud.pub_sub.service import Service
from octue.resources import service_backends


BACKEND_TO_SERVICE_MAPPING = {"GCPPubSubBackend": Service}


class Child:
    """A class representing a child service that can be asked questions. It is a convenience wrapper for `Service` that
    makes the asking of questions more intuitive for Scientists and allows easier selection of backends.

    :param str name: an arbitrary name to refer to the child by (used to access it in Analysis instances and give context to log messages forwarded from the child)
    :param str id: the UUID of the child service
    :param dict backend: must include the key "name" with a value of the name of the type of backend e.g. GCPPubSubBackend and key-value pairs for any other parameters the chosen backend expects
    :return None:
    """

    def __init__(self, name, id, backend):
        self.name = name
        self.id = id

        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)
        self._service = BACKEND_TO_SERVICE_MAPPING[backend_type_name](backend=backend)

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        handle_monitor_message=None,
        timeout=20,
    ):
        """Ask the child a question (i.e. send it some input value and/or a manifest and wait for it to run an analysis
        on them and return the output values). The input values given must adhere to the Twine file of the child.

        :param any input_values: the input values of the question
        :param octue.resources.manifest.Manifest|None input_manifest: the input manifest of the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the remote service and handle them with the local log handlers
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param float timeout: time in seconds to wait for an answer before raising a timeout error
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        subscription, _ = self._service.ask(self.id, input_values, input_manifest, subscribe_to_logs, timeout=timeout)

        return self._service.wait_for_answer(
            subscription=subscription,
            handle_monitor_message=handle_monitor_message,
            service_name=self.name,
            timeout=timeout,
        )
