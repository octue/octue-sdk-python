import logging
import logging.handlers
from urllib.parse import urlparse


def create_formatter(logging_metadata):
    """Create a log formatter from the given logging metadata that delimits the context fields with space-padded
    pipes and encapsulates the whole context in square brackets before adding the message at the end. e.g. if the
    logging metadata was `("%(asctime)s", "%(levelname)s", "%(name)s")`, the formatter would format log messages as e.g.
    `[2021-06-29 11:58:10,985 | INFO | octue.runner] This is a log message.`

    :param iter(str) logging_metadata: an iterable of context fields to use as context for every log message that the formatter is applied to
    :return logging.Formatter:
    """
    return logging.Formatter("[" + " | ".join(logging_metadata) + "]" + " %(message)s")


# Logging format for analysis runs. All handlers should use this logging format, to make logs consistently parseable
LOGGING_METADATA_WITH_TIMESTAMP = ("%(asctime)s", "%(levelname)s", "%(name)s")
FORMATTER_WITH_TIMESTAMP = create_formatter(LOGGING_METADATA_WITH_TIMESTAMP)
FORMATTER_WITHOUT_TIMESTAMP = create_formatter(LOGGING_METADATA_WITH_TIMESTAMP[1:])


def apply_log_handler(logger_name=None, handler=None, log_level=logging.INFO, formatter=None):
    """Apply a log handler with the given formatter to the logger with the given name.

    :param str|None logger_name: if this is `None`, the root logger is used
    :param logging.Handler handler: The handler to use. If None, default console handler will be attached.
    :param int log_level: ignore log messages below this level
    :param logging.Formatter|None formatter: if this is `None`, the default `FORMATTER_WITH_TIMESTAMP` is used
    :return logging.Logger:
    """
    handler = handler or logging.StreamHandler()
    handler.setFormatter(formatter or FORMATTER_WITH_TIMESTAMP)
    handler.setLevel(log_level)
    logger = logging.getLogger(name=logger_name)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    for handler in logger.handlers:
        if type(handler).__name__ == "SocketHandler":
            # Log locally that a remote logger will be used.
            local_logger = logging.getLogger(__name__)
            temporary_handler = logging.StreamHandler()
            temporary_handler.setFormatter(formatter or FORMATTER_WITH_TIMESTAMP)
            temporary_handler.setLevel(log_level)
            local_logger.addHandler(temporary_handler)
            local_logger.setLevel(log_level)
            local_logger.info("Logs streaming to %s:%s", handler.host, str(handler.port))
            local_logger.removeHandler(temporary_handler)
            break

    return logger


def get_remote_handler(logger_uri, formatter=None):
    """Get a log handler for streaming logs to a remote URI accessed via HTTP or HTTPS. The given formatter is applied.

    :param str logger_uri: the URI to stream the logs to
    :param logging.Formatter|None formatter: if this is `None`, the `FORMATTER_WITH_TIMESTAMP` formatter is used
    :return logging.Handler:
    """
    parsed_uri = urlparse(logger_uri)

    if parsed_uri.scheme not in {"ws", "wss"}:
        raise ValueError(
            f"Only WS and WSS protocols currently supported for remote logger URI. Received {logger_uri!r}."
        )

    handler = logging.handlers.SocketHandler(host=parsed_uri.hostname, port=parsed_uri.port)
    handler.setFormatter(formatter or FORMATTER_WITH_TIMESTAMP)
    return handler
