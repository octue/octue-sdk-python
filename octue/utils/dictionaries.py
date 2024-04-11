def make_minimal_dictionary(**kwargs):
    """Make a dictionary with only the keyword arguments that have a non-`None` value.

    :param kwargs: any number of key-value pairs as keyword arguments
    :return dict: a dictionary containing only the key-value pairs that have a non-`None` value
    """
    data = {}

    for key, value in kwargs.items():
        if value is not None:
            data[key] = value

    return data
