import logging
import os

from .log_handlers import apply_log_handler, should_use_octue_log_handler
from .runner import Runner


logger = logging.getLogger(__name__)


PYTHONUNBUFFERED = "PYTHONUNBUFFERED"
__all__ = ("Runner",)


REPOSITORY_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


if should_use_octue_log_handler():
    apply_log_handler(
        logger_name=None,  # Apply to the root logger.
        include_line_number=bool(int(os.environ.get("INCLUDE_LINE_NUMBER_IN_LOGS", 0))),
        include_process_name=bool(int(os.environ.get("INCLUDE_PROCESS_NAME_IN_LOGS", 0))),
        include_thread_name=bool(int(os.environ.get("INCLUDE_THREAD_NAME_IN_LOGS", 0))),
    )

    if not os.environ.get(PYTHONUNBUFFERED):
        logger.warning(
            f"The {PYTHONUNBUFFERED!r} environment variable isn't set - logs may not appear in real time. Set "
            f"{PYTHONUNBUFFERED}=1 to fix this."
        )
