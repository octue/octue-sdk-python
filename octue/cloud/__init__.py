import octue.exceptions
from octue.utils.exceptions import create_exceptions_mapping
import twined.exceptions

EXCEPTIONS_MAPPING = create_exceptions_mapping(
    globals()["__builtins__"],
    vars(twined.exceptions),
    vars(octue.exceptions),
)
