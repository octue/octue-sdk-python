import functools


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
