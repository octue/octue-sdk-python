import logging

from octue.cloud.events.replayer import EventReplayer


logger = logging.getLogger(__name__)


class ChildEmulator:
    """An emulator for the `octue.resources.child.Child` class that handles the given events without contacting the real
    child or using Pub/Sub. Any events a real child could produce are supported. `Child` instances can be
    replaced/mocked like-for-like by `ChildEmulator` without the parent knowing.

    :param list(dict(dict))|None events: the list of events to send to the parent; each event must have an "event" key and an "attributes" key, and all events must conform to the service communication schema
    :param kwargs: any number of keyword arguments that would normally be passed to `Child.__init__`
    :return None:
    """

    def __init__(self, events=None, **kwargs):
        self.events = events or []

        if len(self.events) == 0:
            raise ValueError(
                f"A non-zero number of events must be provided to the child emulator - received {self.events!r}."
            )

        self.id = self.events[0].get("attributes", {}).get("sender")

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

    def ask(self, handle_monitor_message=None, record_events=True, asynchronous=False, **kwargs):
        """Ask the child emulator a question and receive its emulated response events. Unlike a real child, the input
         values and manifest are not validated against the schema in the child's twine as it is only available to the
         real child. Hence, the input values and manifest do not affect the events returned by the emulator.

        :param callable|None handle_monitor_message: a function to handle monitor messages (e.g. send them to an endpoint for plotting or displaying) - this function should take a single JSON-compatible python primitive as an argument (note that this could be an array or object)
        :param bool record_events: if `True`, record events received from the child in the `received_events` property
        :param bool asynchronous: if `True`, don't wait for an answer or create an answer subscription (the result and other events can be retrieved from the event store later)
        :param kwargs: any number of keyword arguments that would normally be passed to `Child.ask`
        :return (dict, str)|(None, str): a dictionary containing the keys "output_values" and "output_manifest" (or `None` if the question is asynchronous), and the question UUID
        """
        question_uuid = self.events[0].get("attributes", {}).get("question_uuid")

        if asynchronous:
            return (None, question_uuid)

        event_replayer = EventReplayer(handle_monitor_message=handle_monitor_message, record_events=record_events)
        result = event_replayer.handle_events(self.events)
        return (result, question_uuid)
