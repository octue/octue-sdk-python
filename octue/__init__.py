import os

from .logging_handlers import FORMATTER_WITHOUT_TIMESTAMP, apply_log_handler
from .runner import Runner


__all__ = ("Runner",)


if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") == "GOOGLE_CLOUD_RUN":
    # Use a log handler with a formatter that doesn't include the timestamp in the log message context to avoid the date
    # appearing twice in the Google Cloud Run logs (Google adds its own timestamp to log messages).
    logger = apply_log_handler(logger_name=None, formatter=FORMATTER_WITHOUT_TIMESTAMP)
else:
    apply_log_handler(logger_name=None)
