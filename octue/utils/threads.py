import time
import warnings
from threading import Timer

from octue.utils.processes import run_logged_subprocess as moved_run_logged_sub_process


class RepeatingTimer(Timer):
    """A repeating version of the `threading.Timer` class."""

    def run(self):
        while not self.finished.is_set():
            time.sleep(self.interval)
            self.function(*self.args, **self.kwargs)


def run_logged_subprocess(command, logger, log_level="info", *args, **kwargs):
    """The deprecated version of `octue.utils.processes.run_logged_subprocess`.

    :param iter(str) command: command to run
    :param logging.Logger logger: logger to use to log stdout and stderr
    :param str log_level: level to log output at
    :raise CalledProcessError: if the subprocess fails (i.e. if it doesn't exit with a 0 return code)
    :return subprocess.CompletedProcess:
    """
    warnings.warn(
        DeprecationWarning(
            "`run_logged_subprocess` has been moved to `octue.utils.processes`. Importing it from "
            "`octue.utils.threads` is deprecated and the ability to do so will be removed soon."
        )
    )

    moved_run_logged_sub_process(command, logger, log_level=log_level, *args, **kwargs)
