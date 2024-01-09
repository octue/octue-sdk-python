import os

from .log_handlers import apply_log_handler, should_use_octue_log_handler
from .runner import Runner


__all__ = ("Runner",)

REPOSITORY_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


if should_use_octue_log_handler():
    apply_log_handler(
        logger_name=None,  # Apply to the root logger.
        include_line_number=bool(int(os.environ.get("INCLUDE_LINE_NUMBER_IN_LOGS", 0))),
        include_process_name=bool(int(os.environ.get("INCLUDE_PROCESS_NAME_IN_LOGS", 0))),
        include_thread_name=bool(int(os.environ.get("INCLUDE_THREAD_NAME_IN_LOGS", 0))),
    )
