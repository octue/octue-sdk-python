import copy

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

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)
        self._service = BACKEND_TO_SERVICE_MAPPING[backend_type_name](name=f"{self.name}-parent", backend=backend)

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        question_uuid=None,
        timeout=20,
    ):
        """Ask the child a question (i.e. send it some input value and/or a manifest and wait for it to run an analysis
        on them and return the output values). The input values given must adhere to the Twine file of the child.

        :param any input_values: the input values of the question
        :param octue.resources.manifest.Manifest|None input_manifest: the input manifest of the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the remote service and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the serving service will have access to these local files
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param float timeout: time in seconds to wait for an answer before raising a timeout error
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :return dict: dictionary containing the keys "output_values" and "output_manifest"
        """
        subscription, _ = self._service.ask(
            service_id=self.id,
            input_values=input_values,
            input_manifest=input_manifest,
            subscribe_to_logs=subscribe_to_logs,
            allow_local_files=allow_local_files,
            question_uuid=question_uuid,
            timeout=timeout,
        )

        return self._service.wait_for_answer(
            subscription=subscription,
            handle_monitor_message=handle_monitor_message,
            service_name=self.name,
            timeout=timeout,
        )
