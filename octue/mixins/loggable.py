import logging


class Loggable:
    """Mixin to allow instantiation of a class with a logger, or by default use the module logger from that class

    The attached logger is a class variable, so all Resources of the same type inheriting from Loggable will share the
    same logger instance; this can be confusing if you overload __init_logger__ in multiple different ways.

    ```
    class MyResource(Loggable):
        def do_something(self):
            self.logger.info('write to a logger')

    MyResource().do_something()  # Log statements go to the default logger for the module in which MyResource is a member
    MyResource(logger=logging.getLogger("specific"))  # Log statements go to the specific logger.
    ```

    :param logging.Logger|None logger:
    :return None:
    """

    def __init__(self, *args, logger=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger
        self.__init_logger__()

    def __init_logger__(self):
        """Overload this in a subclass to initialise your own logger using class attributes.

        :return None:
        """
        self._logger = self._logger or logging.getLogger(self.__class__.__module__)

    @property
    def logger(self):
        return self._logger
