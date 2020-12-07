import logging
import logging.handlers
from urllib.parse import urlparse


# Logging format for analysis runs. All handlers should use this logging format, to make logs consistently parseable
LOG_FORMAT = "%(name)s %(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s"


def get_default_handler(log_level):
    """ Gets a basic console handler set up for logging analyses
    """
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(formatter)
    return console_handler


def get_remote_handler(logger_uri, log_level):
    """Get a log handler for streaming logs to a remote URI accessed via HTTP or HTTPS."""
    parsed_uri = urlparse(logger_uri)

    if parsed_uri.scheme not in {"ws", "wss"}:
        raise ValueError(
            f"Only WS and WSS protocols currently supported for remote logger URI. Received {logger_uri!r}."
        )

    handler = logging.handlers.SocketHandler(host=parsed_uri.hostname, port=parsed_uri.port)
    handler.setLevel(log_level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler
