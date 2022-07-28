import copy
import logging
from unittest.mock import patch

from octue.cloud.emulators.pub_sub import MockService, MockSubscriber, MockSubscription, MockTopic
from octue.resources import Analysis, service_backends


logger = logging.getLogger(__name__)


class ChildEmulator:
    """An emulator for the `Child` class that allows a list of messages to be returned for handling by the parent
    without contacting the real child or using PubSub. Any messages a real child could produce are supported.

    :param str id: the ID of the child
    :param dict backend: must include the key "name" with a value of the name of the type of backend e.g. "GCPPubSubBackend" and key-value pairs for any other parameters the chosen backend expects
    :param str|None internal_service_name: the name to give to the internal service used to ask questions to the child
    :param list(dict)|None messages: the list of messages to send to the parent
    :return None:
    """

    def __init__(self, id, backend, internal_service_name=None, messages=None):
        self.id = id
        self.messages = messages or []

        backend = copy.deepcopy(backend)
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._child = MockService(service_id=self.id, backend=backend, run_function=self._emulate_analysis)

        self._parent = MockService(
            backend=backend,
            service_id=internal_service_name or f"{self.id}-parent",
            children={self._child.id: self._child},
        )

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
        """Ask the child emulator a question and wait for its answer - i.e. send it input values and/or an input
        manifest and wait for it to analyse them and return output values and/or an output manifest. Unlike a real
        child, the input values and manifest are not validated against the schema in the child's twine as it is only
        available to the real child.

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
        with patch("octue.cloud.pub_sub.service.Topic", new=MockTopic):
            with patch("octue.cloud.pub_sub.service.Subscription", new=MockSubscription):
                with patch("octue.resources.child.BACKEND_TO_SERVICE_MAPPING", {"GCPPubSubBackend": MockService}):
                    with patch("google.cloud.pubsub_v1.SubscriberClient", new=MockSubscriber):
                        self._child.serve()

                        subscription, _ = self._parent.ask(
                            service_id=self._child.id,
                            input_values=input_values,
                            input_manifest=input_manifest,
                            subscribe_to_logs=subscribe_to_logs,
                            allow_local_files=allow_local_files,
                            question_uuid=question_uuid,
                        )

                        return self._parent.wait_for_answer(
                            subscription,
                            handle_monitor_message=handle_monitor_message,
                            service_name=self.id,
                            timeout=timeout,
                        )

    def _emulate_analysis(
        self,
        analysis_id,
        input_values,
        input_manifest,
        analysis_log_handler,
        handle_monitor_message,
    ):
        """Emulate analysis of a question by handling the messages given at instantiation in the order given.

        :param str|None analysis_id: UUID of analysis
        :param str|dict|None input_values: the input_values strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param str|dict|octue.resources.manifest.Manifest|None input_manifest: The input_manifest strand data. Can be expressed as a string path of a *.json file (relative or absolute), as an open file-like object (containing json data), as a string of json data or as an already-parsed dict.
        :param logging.Handler|None analysis_log_handler: the logging.Handler instance which will be used to handle logs for this analysis run (this is ignored by the emulator)
        :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
        :return octue.resources.analysis.Analysis:
        """
        for message in self.messages:
            if message["type"] == "log_record":
                logger.handle(message["content"])

            elif message["type"] == "monitor_message":
                handle_monitor_message(message["content"])

            elif message["type"] == "exception":
                raise message["content"]

            elif message["type"] == "result":
                return Analysis(
                    id=analysis_id,
                    twine={},
                    handle_monitor_message=handle_monitor_message,
                    input_values=input_values,
                    input_manifest=input_manifest,
                    output_values=message["content"]["output_values"],
                    output_manifest=message["content"]["output_manifest"],
                )

        # If no result message is included in the given messages, return an empty analysis.
        return Analysis(
            id=analysis_id,
            twine={},
            handle_monitor_message=handle_monitor_message,
            input_values=input_values,
            input_manifest=input_manifest,
            output_values=None,
            output_manifest=None,
        )
