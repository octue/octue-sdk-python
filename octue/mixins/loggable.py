import logging


class Loggable:
    """ Mixin to allow instantiation of a class with a logger, or by default use the module logger from that class

    ```
    class MyResource(Logged):
        def do_something(self):
            self.logger.info('write to a logger')

    MyResource().do_something()  # Log statements go to the default logger for the module in which MyResource is a member
    MyResource(logger=logging.getLogger("specific"))  # Log statements go to the specific logger.
    ```
    """

    def __init__(self, *args, logger=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger or logging.getLogger(self.__class__.__module__)

    @property
    def logger(self):
        return self._logger
