import uuid

from octue.resources import Dataset
from .base import BaseTestCase


class DatafileTestCase(BaseTestCase):
    """ Test case that runs the analyses in the templates.
    """

    def test_instantiates(self):
        """ Ensures a Datafile instantiates using only a path and generates a uuid ID
        """
        resource = Dataset()
        self.assertTrue(isinstance(resource.id, str))
        uuid.UUID(resource.id)


# TODO pull similar tests from Datafiles
