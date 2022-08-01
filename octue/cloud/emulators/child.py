import copy
import json
import logging
import uuid
from unittest.mock import patch

from octue.cloud import EXCEPTIONS_MAPPING
from octue.cloud.emulators.pub_sub import MockService, MockSubscriber, MockSubscription, MockTopic
from octue.resources import Analysis, Manifest, service_backends


logger = logging.getLogger(__name__)

VALID_MESSAGE_TYPES = ("log_record", "monitor_message", "exception", "result")


class ChildEmulator:
    """An emulator for the `octue.resources.child.Child` class that sends the given messages to the parent for handling
    without contacting the real child or using Pub/Sub. Any messages a real child could produce are supported. `Child`
    instances can be replaced/mocked like-for-like by `ChildEmulator` without the parent knowing.

    :param str|None id: the ID of the child; a UUID is generated if none is provided
    :param dict|None backend: a dictionary including the key "name" with a value of the name of the type of backend (e.g. "GCPPubSubBackend") and key-value pairs for any other parameters the chosen backend expects; a mock backend is used if none is provided
    :param str|None internal_service_name: the name to give to the internal service used to ask questions to the child; defaults to "<id>-parent"
    :param list(dict)|None messages: the list of messages to send to the parent
    :return None:
    """

    def __init__(self, id=None, backend=None, internal_service_name=None, messages=None):
        self.id = id or str(uuid.uuid4())
        self.messages = messages or []

        backend = copy.deepcopy(backend or {"name": "GCPPubSubBackend", "project_name": "emulated-project"})
        backend_type_name = backend.pop("name")
        backend = service_backends.get_backend(backend_type_name)(**backend)

        self._child = MockService(service_id=self.id, backend=backend, run_function=self._emulate_analysis)

        self._parent = MockService(
            backend=backend,
            service_id=internal_service_name or f"{self.id}-parent",
            children={self._child.id: self._child},
        )

        self._message_handlers = {
            "log_record": self._handle_log_record,
            "monitor_message": self._handle_monitor_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

    @classmethod
    def from_file(cls, path):
        """Instantiate a child emulator from a JSON file at the given path. All/any/none of the instantiation arguments
        can be given in the file.

        :param str path: the path to a JSON file representing a child emulator
        :return ChildEmulator:
        """
        with open(path) as f:
            serialised_child_emulator = json.load(f)

        return cls(
            id=serialised_child_emulator.get("id"),
            backend=serialised_child_emulator.get("backend"),
            internal_service_name=serialised_child_emulator.get("internal_service_name"),
            messages=serialised_child_emulator.get("messages"),
        )

    def __repr__(self):
        """Represent a child emulator as a string.

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
        """Ask the child emulator a question and receive its emulated response messages. Unlike a real child, the input
         values and manifest are not validated against the schema in the child's twine as it is only available to the
         real child. Hence, the input values and manifest do not affect the messages returned by the emulator.

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
        :param str|dict|None input_values: any input values for the question
        :param str|dict|octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param logging.Handler|None analysis_log_handler: the `logging.Handler` instance which will be used to handle logs for this analysis run (this is ignored by the emulator)
        :param callable|None handle_monitor_message: a function that sends monitor messages to the parent that requested the analysis
        :return octue.resources.analysis.Analysis:
        """
        for message in self.messages:
            self._validate_message(message)
            handler = self._message_handlers[message["type"]]

            result = handler(
                message["content"],
                analysis_id=analysis_id,
                input_values=input_values,
                input_manifest=input_manifest,
                analysis_log_handler=analysis_log_handler,
                handle_monitor_message=handle_monitor_message,
            )

            if result:
                return result

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

    def _validate_message(self, message):
        """Validate the given message to ensure it can be handled.

        :param dict message:
        :raise TypeError: if the message isn't a dictionary
        :raise ValueError: if the message doesn't contain a 'type' key and a 'content' key; if the 'type' key maps to an invalid value
        :return None:
        """
        if not isinstance(message, dict):
            raise TypeError("Each message must be a dictionary.")

        if "type" not in message or "content" not in message:
            raise ValueError(
                f"Each message must contain a 'type' and a 'content' key. The valid types are: {VALID_MESSAGE_TYPES!r}."
            )

        if message["type"] not in VALID_MESSAGE_TYPES:
            raise ValueError(
                f"{message['type']!r} is an invalid message type for the ChildEmulator. The valid types are: "
                f"{VALID_MESSAGE_TYPES!r}."
            )

    def _handle_log_record(self, log_record_dictionary, **kwargs):
        """Convert the given dictionary into a log record and pass it to the log handler.

        :param dict log_record_dictionary: a dictionary representing a log record.
        :param kwargs: this should be empty
        :raise TypeError: if the message can't be converted to a log record
        :return None:
        """
        try:
            log_record_dictionary["levelno"] = log_record_dictionary.get("levelno") or 20
            log_record_dictionary["levelname"] = log_record_dictionary.get("levelname") or "INFO"
            log_record_dictionary["name"] = log_record_dictionary.get("name") or f"{__name__}.{type(self).__name__}"

            logger.handle(logging.makeLogRecord(log_record_dictionary))

        except Exception:
            raise TypeError(
                "The log record must be given as a dictionary that can be converted by `logging.makeLogRecord` to a "
                "`logging.LogRecord` instance."
            )

    def _handle_monitor_message(self, monitor_message, **kwargs):
        """Handle a monitor message with the given handler.

        :param any monitor_message: a monitor message to be handled by the monitor message handler
        :param kwargs: must include the "handle_monitor_message" key
        :return None:
        """
        kwargs.get("handle_monitor_message")(monitor_message)

    def _handle_exception(self, exception, **kwargs):
        """Raise the given exception.

        :param dict|Exception exception: the exception to be raised in python form or serialised form
        :param kwargs: this should be empty
        :raise ValueError: if the given exception cannot be raised
        :return None:
        """
        if isinstance(exception, Exception):
            raise exception

        if "exception_type" not in exception or "exception_message" not in exception:
            raise ValueError(
                "The exception must be given as a dictionary containing the keys 'exception_type' and "
                "'exception_message'."
            )

        try:
            exception_type = EXCEPTIONS_MAPPING[exception["exception_type"]]

        # Allow unknown exception types to still be raised.
        except KeyError:
            exception_type = type(exception["exception_type"], (Exception,), {})

        raise exception_type(exception["exception_message"])

    def _handle_result(self, result, **kwargs):
        """Return the result as an `Analysis` instance.

        :param dict result: a dictionary containing an "output_values" key and an "output_manifest" key
        :param kwargs: must contain the keys "analysis_id", "handle_monitor_message", "input_values", and "input_manifest"
        :raise ValueError: if the result doesn't contain the "output_values" and "output_manifest" keys
        :return octue.resources.analysis.Analysis: an `Analysis` instance containing the emulated outputs
        """
        output_manifest = result.get("output_manifest")

        if output_manifest and not isinstance(output_manifest, Manifest):
            output_manifest = Manifest.deserialise(output_manifest)

        try:
            return Analysis(
                id=kwargs["analysis_id"],
                twine={},
                handle_monitor_message=kwargs["handle_monitor_message"],
                input_values=kwargs["input_values"],
                input_manifest=kwargs["input_manifest"],
                output_values=result["output_values"],
                output_manifest=output_manifest,
            )

        except KeyError:
            raise ValueError(
                "The result must be a dictionary containing the keys 'output_values' and 'output_manifest'."
            )
