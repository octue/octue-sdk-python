import logging
import logging.handlers
from urllib.parse import urlparse


# Logging format for analysis runs. All handlers should use this logging format, to make logs consistently parseable
LOGGING_METADATA = " | ".join(("%(name)s", "%(levelname)s", "%(asctime)s", "%(module)s", "%(process)d", "%(thread)d"))
LOG_FORMAT = "[" + LOGGING_METADATA + "]" + " %(message)s"


def apply_log_handler(logger, handler=None, log_level=logging.INFO):
    """Create a logger specific to the analysis

    :parameter analysis_id: The id of the analysis to get the log for. Should be unique to the analysis
    :type analysis_id: str

    :parameter handler: The handler to use. If None, default console handler will be attached.

    :return: logger named in the pattern `analysis-{analysis_id}`
    :rtype logging.Logger
    """
    handler = handler or get_default_handler(log_level=log_level)
    logger.addHandler(handler)
    logger.setLevel(log_level)
    logger.info("Using local logger.")

    # Log locally that a remote logger will be used from now on.
    if type(logger.handlers[0]).__name__ == "SocketHandler":
        local_logger = logging.getLogger(__name__)
        local_logger.addHandler(get_default_handler(log_level=log_level))
        local_logger.setLevel(log_level)
        local_logger.info("Logs streaming to %s:%s", logger.handlers[0].host, str(logger.handlers[0].port))


def get_default_handler(log_level):
    """ Gets a basic console handler set up for logging analyses. """
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(formatter)
    return console_handler


def get_remote_handler(logger_uri, log_level):
    """ Get a log handler for streaming logs to a remote URI accessed via HTTP or HTTPS. """
    parsed_uri = urlparse(logger_uri)

    if parsed_uri.scheme not in {"ws", "wss"}:
        raise ValueError(
            f"Only WS and WSS protocols currently supported for remote logger URI. Received {logger_uri!r}."
        )

    handler = logging.handlers.SocketHandler(host=parsed_uri.hostname, port=parsed_uri.port)
    handler.setLevel(log_level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler
