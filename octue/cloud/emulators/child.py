import json
import logging
import warnings

from octue.cloud.events.replayer import EventReplayer


logger = logging.getLogger(__name__)


class ChildEmulator:
    """An emulator for the `octue.resources.child.Child` class that sends the given events to the parent for handling
    without contacting the real child or using Pub/Sub. Any events a real child could produce are supported. `Child`
    instances can be replaced/mocked like-for-like by `ChildEmulator` without the parent knowing.

    :param list(dict)|None events: the list of events to send to the parent
    :return None:
    """

    def __init__(self, events=None, **kwargs):
        self.events = events or []

        if len(self.events) == 0:
            raise ValueError(f"Events must be provided to the child emulator - received {self.events!r}.")

        self.id = self.events[0].get("attributes", {}).get("sender")

    @classmethod
    def from_file(cls, path):
        """Instantiate a child emulator from a JSON file at the given path. All/any/none of the instantiation arguments
        can be given in the file.

        :param str path: the path to a JSON file representing a child emulator
        :return ChildEmulator:
        """
        with open(path) as f:
            serialised_child_emulator = json.load(f)

        if "messages" in serialised_child_emulator:
            events = serialised_child_emulator["messages"]

            warnings.warn(
                "Use of 'messages' as a key in an events JSON file for a child emulator is deprecated, and support for "
                "it will be removed soon. Please use 'events' for the key instead.",
                category=DeprecationWarning,
            )

        else:
            events = serialised_child_emulator.get("events")

        return cls(id=serialised_child_emulator.get("id"), events=events)

    def __repr__(self):
        """Represent a child emulator as a string.

        :return str:
        """
        return f"<{type(self).__name__}({self.id!r})>"

    @property
    def received_events(self):
        """Get the events received from the child.

        :return list(dict):
        """
        return self.events

    def ask(
        self,
        input_values=None,
        input_manifest=None,
        subscribe_to_logs=True,
        allow_local_files=False,
        handle_monitor_message=None,
        record_events=True,
        question_uuid=None,
        parent_question_uuid=None,
        originator_question_uuid=None,
        originator=None,
        push_endpoint=None,
        asynchronous=False,
        retry_count=0,
        timeout=86400,
    ):
        """Ask the child emulator a question and receive its emulated response events. Unlike a real child, the input
         values and manifest are not validated against the schema in the child's twine as it is only available to the
         real child. Hence, the input values and manifest do not affect the events returned by the emulator.

        :param any|None input_values: any input values for the question
        :param octue.resources.manifest.Manifest|None input_manifest: an input manifest of any datasets needed for the question
        :param bool subscribe_to_logs: if `True`, subscribe to logs from the child and handle them with the local log handlers
        :param bool allow_local_files: if `True`, allow the input manifest to contain references to local files - this should only be set to `True` if the child will have access to these local files
        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_events: if `True`, record events received from the child in the `received_events` property
        :param str|None question_uuid: the UUID to use for the question if a specific one is needed; a UUID is generated if not
        :param str|None parent_question_uuid: the UUID of the question that triggered this question
        :param str|None originator_question_uuid: the UUID of the question that triggered all ancestor questions of this question
        :param str|None originator: the SRUID of the service revision that triggered all ancestor questions of this question
        :param str|None push_endpoint: if answers to the question should be pushed to an endpoint, provide its URL here (the returned subscription will be a push subscription); if not, leave this as `None`
        :param bool asynchronous: if `True`, don't create an answer subscription
        :param int retry_count: the retry count of the question (this is zero if it's the first attempt at the question)
        :param float timeout: time in seconds to wait for an answer before raising a timeout error
        :raise TimeoutError: if the timeout is exceeded while waiting for an answer
        :return dict, str: a dictionary containing the keys "output_values" and "output_manifest", and the question UUID
        """
        event_replayer = EventReplayer(handle_monitor_message=handle_monitor_message, record_events=record_events)
        result = event_replayer.handle_events(self.events)
        return (result, self.events[0].get("attributes", {}).get("question_uuid"))
