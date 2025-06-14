import sys
import traceback as tb


def create_exceptions_mapping(*sources):
    """Create a single mapping of exception names to their classes given any number of dictionaries mapping variable
    names to variables e.g. `locals()`, `globals()` or a module. Non-exception variables are filtered out. This function
    can be used to combine several modules of exceptions into one mapping.

    :param sources: any number of `dict`s of global or local variables mapping object names to objects
    :return dict:
    """
    candidates = {key: value for source in sources for key, value in source.items()}

    exceptions_mapping = {}

    for name, object in candidates.items():
        try:
            if issubclass(object, BaseException):
                exceptions_mapping[name] = object

        except TypeError:
            continue

    return exceptions_mapping


def convert_exception_to_primitives(exception=None):
    """Convert an exception into a dictionary of its type, message, and traceback as JSON-serialisable primitives. The
    exception is acquired using `sys.exc_info` if one is not supplied.

    :param Exception|None exception: the exception to convert; if `None`, the exception is acquired using `sys.exc_info`
    :return dict: a dictionary with "type", "message" and "traceback" keys and JSON-serialisable values
    """
    if exception:
        try:
            raise exception
        except Exception:
            exception_info = sys.exc_info()
    else:
        exception_info = sys.exc_info()

    return {
        "type": exception_info[0].__name__,
        "message": f"{exception_info[1]}",
        "traceback": tb.format_list(tb.extract_tb(exception_info[2])),
    }


def convert_exception_event_to_exception(event, sender, exceptions_mapping):
    exception_message = "\n\n".join(
        (
            event["exception_message"],
            f"The following traceback was captured from the remote service {sender!r}:",
            "".join(event["exception_traceback"]),
        )
    )

    try:
        exception_type = exceptions_mapping[event["exception_type"]]

    # Allow unknown exception types to still be raised.
    except KeyError:
        exception_type = type(event["exception_type"], (Exception,), {})

    return exception_type(exception_message)
