import logging
import logging.handlers
from urllib.parse import urlparse


# Logging format for analysis runs. All handlers should use this logging format, to make logs consistently parseable
LOG_FORMAT = "%(name)s %(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s"


def get_default_handler(_log_level):
    """ Gets a basic console handler set up for logging analyses
    """
    console_handler = logging.StreamHandler()
    console_handler.setLevel(_log_level)
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(formatter)
    return console_handler


def get_remote_logger_handler(logger_uri):
    parsed_uri = urlparse(logger_uri)

    return logging.handlers.HTTPHandler(host=parsed_uri.netloc, url=parsed_uri.path, method="POST")
