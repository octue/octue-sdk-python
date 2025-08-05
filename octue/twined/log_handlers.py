from octue.log_handlers import (
    apply_log_handler,
    create_octue_formatter,
    get_log_record_attributes_for_environment,
    should_use_octue_log_handler,
)


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
