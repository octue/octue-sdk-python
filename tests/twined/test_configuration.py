from octue.twined.configuration import ServiceConfiguration
from tests.base import BaseTestCase


class TestServiceConfiguration(BaseTestCase):
    def test_error_raised_if_namespace_disallowed(self):
        """Test that an error is raised if a disallowed namespace is given."""
        with self.assertRaises(ValueError) as error_context:
            ServiceConfiguration(namespace="example", name="service")

        self.assertEqual(error_context.exception.args[0], "'example' is not an allowed Twined service namespace.")
