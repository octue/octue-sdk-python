class EventCounter:
    """A mutable counter for keeping track of the emission order of events. This is used in the `Service` class instead
    of an integer because it is mutable and can be passed to the `Service._emit_event` method and incremented as
    events are emitted.

    :return None:
    """

    def __init__(self):
        self.count = 0

    def __iadd__(self, other):
        """Increment the counter by an integer.

        :return octue.cloud.events.counter.EventCounter: the event counter with its count updated
        """
        if not isinstance(other, int):
            raise ValueError(f"Event counters can only be incremented by an integer; received {other!r}.")

        self.count += other
        return self

    def __int__(self):
        """Get the counter as an integer.

        :return int: the counter as an integer
        """
        return int(self.count)

    def __repr__(self):
        """Represent the counter as a string.

        :return str: the counter represented as a string.
        """
        return f"<{type(self).__name__}(count={self.count})"
