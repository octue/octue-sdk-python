import time
from threading import Timer


class RepeatingTimer(Timer):
    """A repeating version of the `threading.Timer` class."""

    def run(self):
        while not self.finished.is_set():
            time.sleep(self.interval)
            self.function(*self.args, **self.kwargs)
