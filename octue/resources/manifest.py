import logging

from octue.mixins import Identifiable, Loggable, Serialisable


module_logger = logging.getLogger(__name__)


class Manifest(Serialisable, Loggable, Identifiable):
    """ A representation of a manifest, which can contain multiple datasets
    This is used to manage all files coming into (or leaving), a data service for an analysis at the
    configuration, input or output stage.
    """

    def __init__(self, id=None, logger=None, **kwargs):
        """ Construct a Manifest
        """
        super().__init__(id=id, logger=logger)
        self.datasets = kwargs.pop("datasets", list())
        self.__dict__.update(**kwargs)
