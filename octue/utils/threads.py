import time
from subprocess import PIPE, STDOUT, CalledProcessError, Popen
from threading import Thread, Timer


class RepeatingTimer(Timer):
    """A repeating version of the `threading.Timer` class."""

    def run(self):
        while not self.finished.is_set():
            time.sleep(self.interval)
            self.function(*self.args, **self.kwargs)


def run_logged_subprocess(command, logger, log_level="info", *args, **kwargs):
    """Run a subprocess, sending its stdout and stderr output to the given logger. Extra `args` and `kwargs` are
    provided to the `subprocess.Popen` instance used.

    :param iter(str) command: command to run
    :param logging.Logger logger: logger to use to log stdout and stderr
    :param str log_level: level to log output at
    :raise CalledProcessError: if the subprocess fails (i.e. if it doesn't exit with a 0 return code)
    :return subprocess.CompletedProcess:
    """

    def _log_lines_from_stream(stream, logger):
        """Log lines from the given stream.

        :param io.BufferedReader stream:
        :param logging.Logger logger:
        :return None:
        """
        with stream:
            for line in iter(stream.readline, b""):
                getattr(logger, log_level.lower())(line.decode().strip())

    process = Popen(command, stdout=PIPE, stderr=STDOUT, *args, **kwargs)
    Thread(target=_log_lines_from_stream, args=[process.stdout, logger]).start()
    process.wait()

    if process.returncode != 0:
        raise CalledProcessError(returncode=process.returncode, cmd=" ".join(command))

    return process
