import logging

from octue.mixins import Loggable
from ..base import BaseTestCase


class InheritLoggable(Loggable):
    """Class used purely for testing to check the logger is instantiated"""

    pass


class LoggableTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """Ensures the class instantiates without arguments"""
        resource = Loggable()
        self.assertIsInstance(resource.logger, logging.Logger)
        self.assertEqual(resource.logger.name, "octue.mixins.loggable")

    def test_inherits_correct_module_name(self):
        """Ensures default logger is the one attached to the file where the derived class was declared,
        not where the Loggable class was declared
        """
        resource = InheritLoggable()
        self.assertIsInstance(resource.logger, logging.Logger)
        self.assertEqual(resource.logger.name, __name__)

    def test_assigns_if_logger_passed(self):
        """Ensures non-default logger is attached correctly"""
        custom_logger = logging.getLogger("custom_logger")
        resource = InheritLoggable(logger=custom_logger)
        self.assertIsInstance(resource.logger, logging.Logger)
        self.assertEqual(resource.logger.name, "custom_logger")
