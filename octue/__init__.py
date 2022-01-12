import os

from .log_handlers import apply_log_handler
from .runner import Runner


__all__ = ("Runner",)
REPOSITORY_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


if int(os.environ.get("USE_OCTUE_LOG_HANDLER", "0")) == 1:
    # Use the default log handler from this package if `USE_OCTUE_LOG_HANDLER` is 1. The default value for this is 0
    # because `octue` is a package that is primarily imported - the importer may not want to use this log handler if
    # they have their own.
    apply_log_handler(
        logger_name=None,  # Apply to the root logger.
        include_line_number=bool(int(os.environ.get("INCLUDE_LINE_NUMBER_IN_LOGS", 0))),
        include_process_name=bool(int(os.environ.get("INCLUDE_PROCESS_NAME_IN_LOGS", 0))),
        include_thread_name=bool(int(os.environ.get("INCLUDE_THREAD_NAME_IN_LOGS", 0))),
    )
