from coolname import generate_slug


class CoolNameable:
    """A mixin that, if the subclass doesn't have an existing `name`, gives it a highly unique human-readable name such
    as `melodic-kestrel`.
    """

    def __init__(self, *args, **kwargs):
        existing_name = getattr(self, "name", None) or kwargs.pop("name", None)

        if existing_name:
            self.name = existing_name
            self._cool_named = False
        else:
            self.name = generate_slug(2)
            self._cool_named = True

        super().__init__(*args, **kwargs)
