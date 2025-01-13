import importlib.metadata

import octue.exceptions
import twined.exceptions
from octue.utils.exceptions import create_exceptions_mapping


LOCAL_SDK_VERSION = importlib.metadata.version("octue")


EXCEPTIONS_MAPPING = create_exceptions_mapping(
    globals()["__builtins__"],
    vars(twined.exceptions),
    vars(octue.exceptions),
)
