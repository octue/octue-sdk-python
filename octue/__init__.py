import os

from .log_handlers import apply_log_handler, create_octue_formatter, get_metadata_schema_for_current_compute_platform
from .runner import Runner


__all__ = ("Runner",)


if int(os.environ.get("USE_OCTUE_LOG_HANDLER", "0")) == 1:
    # Use the default log handler from this package if `USE_OCTUE_LOG_HANDLER` is 1. The default value for this is 0
    # because `octue` is a package that is primarily imported - the importer may not want to use this log handler if
    # they have their own.
    logging_metadata_schema = get_metadata_schema_for_current_compute_platform()
    apply_log_handler(logger_name=None, formatter=create_octue_formatter(logging_metadata_schema))
