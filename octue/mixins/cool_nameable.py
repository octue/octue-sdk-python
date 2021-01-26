from coolname import generate_slug


class CoolNameable:
    """ A mixin that gives the subclass a highly unique human-readable name e.g. "melodic-kestrel". """

    def __init__(self, *args, **kwargs):
        name = kwargs.pop("name", None)

        if name is not None:
            self.name = name
            self._cool_named = False
        else:
            self.name = generate_slug(2)
            self._cool_named = True
