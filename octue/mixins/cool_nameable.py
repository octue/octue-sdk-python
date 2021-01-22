from coolname import generate_slug


class CoolNameable:
    """ A mixin that gives the subclass a highly unique human-readable name e.g. "melodic-kestrel". """

    def __init__(self):
        self.cool_name = generate_slug(2)
