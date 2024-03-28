from octue.exceptions import BackendNotFound, CloudLocationNotSpecified
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
        self.assertEqual(
            repr(GCPPubSubBackend(project_name="hello", services_namespace="world")),
            "<GCPPubSubBackend(project_name='hello', services_namespace='world')>",
        )

    def test_error_raised_if_project_name_or_services_namespace_is_none(self):
        """Test that an error is raised if the project name or services namespace aren't given during `GCPPubSubBackend`
        instantiation.
        """
        for project_name, services_namespace in (("hello", None), (None, "world")):
            with self.subTest(project_name=project_name, services_namespace=services_namespace):
                with self.assertRaises(CloudLocationNotSpecified):
                    GCPPubSubBackend(project_name=project_name, services_namespace=services_namespace)
