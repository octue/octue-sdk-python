import os

from .logging_handlers import FORMATTER_WITHOUT_TIMESTAMP, apply_log_handler
from .runner import Runner


__all__ = ("Runner",)


if os.environ.get("PLATFORM") == "GOOGLE_CLOUD_RUN":
    # Use a log handler with a formatter that doesn't include the timestamp in the log message context to avoid the date
    # appearing twice in the Google Cloud Run logs (Google adds its own timestamp to log messages).
    logger = apply_log_handler(__name__, formatter=FORMATTER_WITHOUT_TIMESTAMP)
else:
    apply_log_handler()
