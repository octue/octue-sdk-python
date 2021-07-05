import os

from .log_handlers import FORMATTER_WITHOUT_TIMESTAMP, apply_log_handler
from .runner import Runner


__all__ = ("Runner",)


if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") == "GOOGLE_CLOUD_RUN":
    # Use a log handler with a formatter that doesn't include the timestamp in the log message context to avoid the date
    # appearing twice in the Google Cloud Run logs (Google adds its own timestamp to log messages).
    logger = apply_log_handler(logger_name=None, formatter=FORMATTER_WITHOUT_TIMESTAMP)
elif int(os.environ.get("USE_OCTUE_LOG_HANDLER", "0")) == 1:
    # Use the default log handler from this package if `USE_OCTUE_LOG_HANDLER` is 1. The default value for this is 0
    # because `octue` is a package that is primarily imported - the importer may not want to use this log handler if
    # they have their own.
    apply_log_handler(logger_name=None)
