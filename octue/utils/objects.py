import functools
import itertools


def get_nested_attribute(instance, nested_attribute_name):
    """Get the value of a nested attribute from a class instance or dictionary, with each level of nesting being
    another dictionary or class instance.

    :param dict|object instance:
    :param str nested_attribute_names: dot-separated nested attribute name e.g. "a.b.c", "a.b", or "a"
    :return any:
    """
    nested_attribute_names = nested_attribute_name.split(".")
    return functools.reduce(getattr_or_subscribe, nested_attribute_names, instance)


def has_nested_attribute(instance, nested_attribute_name):
    """Check if a class instance or dictionary has a nested attribute with the given name (each level of nesting being
    another dictionary or class instance).

    :param dict|object instance:
    :param str nested_attribute_names: dot-separated nested attribute name e.g. "a.b.c", "a.b", or "a"
    :return bool:
    """
    try:
        get_nested_attribute(instance, nested_attribute_name)
    except AttributeError:
        return False
    return True


def getattr_or_subscribe(instance, name):
    """Get an attribute from a class instance or a value from a dictionary.

    :param dict|object instance:
    :param str name: name of attribute or dictionary key
    :return any:
    """
    try:
        return getattr(instance, name)
    except AttributeError:
        try:
            return instance[name]
        except (TypeError, KeyError):
            raise AttributeError(f"{instance!r} does not have an attribute or key named {name!r}.")


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
