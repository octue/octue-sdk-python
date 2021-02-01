class MixinBase:
    """Allows you to use any combination of mixin classes which pass unused *args and **kwargs up to their superclass,
    without encountering:
    ```
    Error
    Traceback (most recent call last):
        super().__init__(*args, **kwargs)
    TypeError: object.__init__() takes no parameters
    ```
    on initialisation.

    The case of passing extra args and kwargs to your class constructor which aren't popped by the various
    mixins is still handled safely with the same error (to prevent loss of variables and poorly defined behaviour
    later).
    """

    def __init__(self, *args, **kwargs):
        """Constructor for ResourceBase"""
        if (len(args) > 0) or (len(kwargs.keys()) > 0):
            raise TypeError("object.__init__() takes no parameters")
        super().__init__()
