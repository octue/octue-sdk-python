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
