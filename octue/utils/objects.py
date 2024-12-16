import itertools

import pydash


def get_nested_attribute(instance, name):
    """Get the value of a nested attribute from a class instance or dictionary, with each level of nesting being
    another dictionary or class instance.

    :param dict|object instance:
    :param str name: a dot-separated nested attribute name e.g. "a.b.c", "a.b", or "a"
    :return any:
    """
    # This is a random number used as a default instead of `None` to signal that an attribute is missing rather than
    # existing and just having `None` as its value.
    missing_indicator = "7027907024295393"
    attribute = pydash.get(instance, name, default=missing_indicator)

    if attribute == missing_indicator:
        raise AttributeError(f"{instance!r} does not have an attribute or key named {name!r}.")

    return attribute


def dictionary_product(keep_none_values=False, **kwargs):
    """Yield from the Cartesian product of the kwargs' iterables, optionally including `None`-valued kwargs. This
    function acts like `itertools.product` but yields each combination as a dictionary instead of a tuple. The
    dictionaries have the kwargs' keys as their keys.

    :param bool keep_none_values: if `True`, include `None`-valued kwargs in the yielded combinations
    :param kwargs: any number of iterables
    :yield dict: one of the combinations of the kwargs' iterables as a dictionary
    """
    iterables = {}
    nones = {}

    # Put kwargs with `None` values in a separate dictionary to kwargs with non-`None` values.
    for key, value in kwargs.items():
        if value is None:
            nones[key] = value
        else:
            iterables[key] = value

    # Yield each possible combination from the Cartesian product of the kwargs' iterables.
    for combination in itertools.product(*iterables.values()):
        combination_dict = dict(zip(iterables.keys(), combination))

        # Optionally add the `None`-valued kwargs to what's yielded.
        if keep_none_values:
            combination_dict = {**combination_dict, **nones}

        yield combination_dict
