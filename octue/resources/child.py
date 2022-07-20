import copy

from octue.cloud.pub_sub.service import Service
from octue.resources import service_backends


BACKEND_TO_SERVICE_MAPPING = {"GCPPubSubBackend": Service}


class Child:
    """A class representing an Octue child service that can be asked questions. It is a convenience wrapper for
    `Service` that makes question asking more intuitive and allows easier selection of backends.

    :param str id: the ID of the child
    :param dict backend: must include the key "name" with a value of the name of the type of backend e.g. "GCPPubSubBackend" and key-value pairs for any other parameters the chosen backend expects
    :param str|None internal_service_name: the name to give to the internal service used to ask questions to the child
    :return None:
    """

    def __init__(self, id, backend, internal_service_name=None):
        self.id = id

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._service = BACKEND_TO_SERVICE_MAPPING[backend_type_name](
            name=internal_service_name or f"{self.id}-parent",
            backend=backend,
        )

    def __repr__(self):
        """Represent the child as a string.

        :return str:
        """
        return f"<{type(self).__name__}({self.id!r})>"

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        question_uuid=None,
        timeout=86400,
    ):
        """Ask the child a question and wait for its answer - i.e. send it input values and/or an input manifest and
        wait for it to analyse them and return output values and/or an output manifest. The input values and manifest
        must conform to the schema in the child's twine.

        :param any|None input_values: any input values for the question
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the child and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will have access to these local files
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param float timeout: time in seconds to wait for an answer before raising a timeout error
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :return dict: a dictionary containing the keys "output_values" and "output_manifest"
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
            service_name=self.id,
            timeout=timeout,
        )
