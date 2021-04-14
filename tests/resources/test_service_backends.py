from octue.exceptions import BackendNotFound
from octue.resources.service_backends import GCPPubSubBackend, get_backend
from tests.base import BaseTestCase


class TestServiceBackends(BaseTestCase):
    def test_error_raised_when_backend_type_does_not_exist(self):
        """Test that an error is raised if the given backend name doesn't correspond to an existing backend type."""
        with self.assertRaises(BackendNotFound):
            get_backend("blahblah")

    def test_existing_backend_can_be_retrieved(self):
        """Test that an existing backend can be retrieved."""
        backend = get_backend("GCPPubSubBackend")
        self.assertEqual(backend.__name__, "GCPPubSubBackend")

    def test_repr(self):
        """Test the representation displays as expected."""
        self.assertEqual(repr(GCPPubSubBackend(project_name="hello")), "<GCPPubSubBackend(project_name='hello')>")
