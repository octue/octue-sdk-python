import logging
import logging.handlers
import os
from urllib.parse import urlparse

from octue.definitions import GOOGLE_COMPUTE_PROVIDERS

if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") in GOOGLE_COMPUTE_PROVIDERS:
    # Google Cloud logs don't support colour currently - provide a no-operation function.
    colourise = lambda string, text_colour=None, background_colour=None: string
else:
    from octue.utils.colour import colourise  # noqa


# Logging format for analysis runs. All handlers should use this logging format to make logs consistently parseable.
LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP = ["%(asctime)s", "%(levelname)s", "%(name)s"]
LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP = LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP[1:]

# "colorblind" colour palette from seaborn/matplotlib.
COLOUR_PALETTE = ["0173b2", "de8f05", "029e73", "d55e00", "cc78bc", "ca9161", "fbafe4", "949494", "ece133", "56b4e9"]


def should_use_octue_log_handler():
    """Return `True` if the `USE_OCTUE_LOG_HANDLER` environment variable is set to "1" or is unspecified; return `False`
    if it's set to "0" or anything else.

    :return bool: `True` if the `USE_OCTUE_LOG_HANDLER` environment variable is set to "1" or is unspecified
    """
    return int(os.environ.get("USE_OCTUE_LOG_HANDLER", "1")) == 1


def create_octue_formatter(
    *log_record_attributes,
    include_line_number=False,
    include_process_name=False,
    include_thread_name=False,
    use_colour=True,
):
    """Create a log formatter from the given log record attributes that delimits the attributes with space-padded pipes
    and encapsulates the whole log message context in square brackets before adding the message at the end. e.g. if the
    attributes are `["%(asctime)s", "%(levelname)s", "%(name)s"]`, the formatter would format log messages as e.g.
    `[2021-06-29 11:58:10,985 | INFO | octue.runner] This is a log message.`

    :param log_record_attributes: any number of iterables of log record attribute names to use as context for every log message that the formatter is applied to; each iterable is interpreted as a different section by the formatter
    :param bool include_line_number: if `True`, include the line number in the log context
    :param bool include_process_name: if `True`, include the process name in the log context
    :param bool include_thread_name: if `True`, include the thread name in the log context
    :param bool use_colour: if `True`, use ANSI colour codes to colour the logs
    :return logging.Formatter:
    """
    global colourise

    extra_attributes = []

    if include_line_number:
        extra_attributes.append("%(lineno)d")
    if include_process_name:
        extra_attributes.append("%(processName)s")
    if include_thread_name:
        extra_attributes.append("%(threadName)s")

    if not use_colour:
        colourise = lambda string, text_colour=None, background_colour=None: string

    if len(log_record_attributes) > 1:
        extra_sections = [
            " ".join(
                colourise(
                    "[" + " | ".join(attributes_section) + "]",
                    text_colour=COLOUR_PALETTE[2],
                )
                for attributes_section in log_record_attributes[1:]
            )
        ]

    else:
        extra_sections = []

    return logging.Formatter(
        " ".join(
            [
                colourise(
                    "[" + " | ".join(log_record_attributes[0] + extra_attributes) + "]",
                    text_colour=COLOUR_PALETTE[0],
                ),
                *extra_sections,
                "%(message)s",
            ]
        )
    )


def apply_log_handler(
    logger_name=None,
    logger=None,
    handler=None,
    log_level=logging.INFO,
    formatter=None,
    include_line_number=False,
    include_process_name=False,
    include_thread_name=False,
):
    """Apply a log handler with the given formatter to the logger with the given name. By default, the default Octue log
    handler is used on the root logger.

    :param str|None logger_name: the name of the logger to apply the handler to; if this and `logger` are `None`, the root logger is used
    :param logging.Logger|None logger: the logger instance to apply the handler to (takes precedence over a logger name)
    :param logging.Handler|None handler: The handler to use. If `None`, the default `StreamHandler` will be attached.
    :param int|str log_level: ignore log messages below this level
    :param logging.Formatter|None formatter: if provided, this formatter is used and the other formatting options are ignored
    :param bool include_line_number: if `True`, include the line number in the log context
    :param bool include_process_name: if `True`, include the process name in the log context
    :param bool include_thread_name: if `True`, include the thread name in the log context
    :return logging.Handler:
    """
    logger = logger or logging.getLogger(name=logger_name)
    handler = handler or logging.StreamHandler()

    for existing_handler in logger.handlers:
        if type(existing_handler).__name__ == "StreamHandler" and type(handler).__name__ == "StreamHandler":
            logger.removeHandler(existing_handler)

    if formatter is None:
        formatter = create_octue_formatter(
            get_log_record_attributes_for_environment(),
            include_line_number=include_line_number,
            include_process_name=include_process_name,
            include_thread_name=include_thread_name,
        )

    handler.setFormatter(formatter)
    handler.setLevel(log_level)

    logger.addHandler(handler)
    logger.setLevel(log_level)

    for handler in logger.handlers:
        if type(handler).__name__ == "SocketHandler":
            # Log locally that a remote logger will be used.
            local_logger = logging.getLogger(__name__)
            temporary_handler = logging.StreamHandler()
            temporary_handler.setFormatter(formatter)
            temporary_handler.setLevel(log_level)
            local_logger.addHandler(temporary_handler)
            local_logger.setLevel(log_level)
            local_logger.info("Logs streaming to %s:%s", handler.host, str(handler.port))
            local_logger.removeHandler(temporary_handler)
            break

    return handler


def get_remote_handler(
    logger_uri,
    formatter=None,
    include_line_number=False,
    include_process_name=False,
    include_thread_name=False,
):
    """Get a log handler for streaming logs to a remote URI accessed via HTTP or HTTPS. The default octue log formatter
    is used if no formatter is provided.

    :param str logger_uri: the URI to stream the logs to
    :param logging.Formatter|None formatter: if provided, this formatter is used and the other formatting options are ignored
    :param bool include_line_number: if `True`, include the line number in the log context
    :param bool include_process_name: if `True`, include the process name in the log context
    :param bool include_thread_name: if `True`, include the thread name in the log context
    :return logging.Handler:
    """
    parsed_uri = urlparse(logger_uri)

    if parsed_uri.scheme not in {"ws", "wss"}:
        raise ValueError(
            f"Only WS and WSS protocols currently supported for remote logger URI. Received {logger_uri!r}."
        )

    handler = logging.handlers.SocketHandler(host=parsed_uri.hostname, port=parsed_uri.port)

    formatter = formatter or create_octue_formatter(
        get_log_record_attributes_for_environment(),
        include_line_number=include_line_number,
        include_process_name=include_process_name,
        include_thread_name=include_thread_name,
    )

    handler.setFormatter(formatter)
    return handler


def get_log_record_attributes_for_environment():
    """Get the correct log record attributes for the environment. If the environment is in Google Cloud, get log record
    attributes not including the timestamp in the log context to avoid the date appearing twice in the Google Cloud
    logs (Google adds its own timestamp to log messages). Otherwise, get log record attributes including the timestamp.

    :return list:
    """
    if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") in GOOGLE_COMPUTE_PROVIDERS:
        return LOG_RECORD_ATTRIBUTES_WITHOUT_TIMESTAMP

    return LOG_RECORD_ATTRIBUTES_WITH_TIMESTAMP


class AnalysisLogFormatterSwitcher:
    """A context manager that, when activated, removes any formatters from the logger's handlers, adds any extra
    handlers provided, and adds formatters that include the analysis ID to all the handlers. On leaving the context, the
    logger is restored to its initial state.

    If the `USE_OCTUE_LOG_HANDLER` environment variable is not set to "1" and the logger has handlers, their formatters
    are left unchanged and the extra log handlers are added with the logger's zeroth handler's formatter applied to
    them.

    :param str analysis_id: the ID of the analysis to add to the log formatters
    :param logger.Logger logger: the logger whose handlers' formatters should be switched
    :param str|int analysis_log_level: the log level to apply to any extra log handlers
    :param list(logging.Handler) extra_log_handlers: any extra log handlers to add to the logger
    :return None:
    """

    def __init__(self, analysis_id, logger, analysis_log_level, extra_log_handlers=None):
        self.analysis_id = analysis_id
        self.logger = logger
        self.analysis_log_level = analysis_log_level
        self.extra_log_handlers = extra_log_handlers or []
        self.initial_formatters = [(handler, handler.formatter) for handler in self.logger.handlers]

        if should_use_octue_log_handler() or not self.logger.handlers:
            # Create formatters that include the analysis ID in the logging metadata.
            self.coloured_analysis_formatter = create_octue_formatter(
                get_log_record_attributes_for_environment(),
                [self.analysis_id],
            )

            self.uncoloured_analysis_formatter = create_octue_formatter(
                get_log_record_attributes_for_environment(),
                [self.analysis_id],
                use_colour=False,
            )

        else:
            self.coloured_analysis_formatter = self.logger.handlers[0].formatter
            self.uncoloured_analysis_formatter = self.logger.handlers[0].formatter

    def __enter__(self):
        """Carry out the following:

        1. Remove the initial formatters from the logger's handlers
        2. Add the analysis formatter to the logger's current handlers and any extra handlers
        3. Add the extra handlers to the logger

        :return None:
        """
        if should_use_octue_log_handler():
            self._remove_formatters()
            self._add_analysis_formatter_to_log_handlers()

        self._add_extra_handlers()

    def __exit__(self, *args):
        """Carry out the following:

        1. Remove the extra handlers from the logger
        2. Remove the analysis log formatter from the logger's initial handlers
        3. Restore the initial formatters to the logger's initial handlers

        :return None:
        """
        self._remove_extra_handlers()

        if should_use_octue_log_handler():
            self._remove_formatters()
            self._restore_initial_formatters()

    def _remove_formatters(self):
        """Remove the formatters from any handlers the logger currently has.

        :return None:
        """
        for handler in self.logger.handlers:
            handler.formatter = None

    def _remove_extra_handlers(self):
        """Remove any extra handlers from the logger.

        :return None:
        """
        for handler in self.extra_log_handlers:
            self.logger.removeHandler(handler)

    def _add_analysis_formatter_to_log_handlers(self):
        """Apply the coloured analysis formatter to the logger's current handlers.

        :return None:
        """
        for handler in self.logger.handlers:
            handler.setFormatter(self.coloured_analysis_formatter)

    def _add_extra_handlers(self):
        """Add any extra log handlers to the logger and add the relevant analysis formatter to them.

        :return None:
        """
        if not self.extra_log_handlers:
            return

        # Apply any other given handlers to the logger.
        for extra_handler in self.extra_log_handlers:
            # Apply the uncoloured analysis formatter to any file log handlers to keep them readable (i.e. to avoid the
            # ANSI escape codes).
            if type(extra_handler).__name__ == "FileHandler":
                apply_log_handler(
                    logger=self.logger,
                    handler=extra_handler,
                    log_level=self.analysis_log_level,
                    formatter=self.uncoloured_analysis_formatter,
                )

            elif (
                type(extra_handler).__name__ == "MemoryHandler"
                and getattr(extra_handler, "target")
                and type(getattr(extra_handler, "target")).__name__ == "FileHandler"
            ):
                apply_log_handler(
                    logger=self.logger,
                    handler=extra_handler.target,
                    log_level=self.analysis_log_level,
                    formatter=self.uncoloured_analysis_formatter,
                )

            else:
                # Apply the coloured analysis formatter to any other types of log handler.
                apply_log_handler(
                    logger=self.logger,
                    handler=extra_handler,
                    log_level=self.analysis_log_level,
                    formatter=self.coloured_analysis_formatter,
                )

    def _restore_initial_formatters(self):
        """Restore the initial formatters to the logger's initial handlers.

        :return None:
        """
        for handler, formatter in self.initial_formatters:
            handler.setFormatter(formatter)
