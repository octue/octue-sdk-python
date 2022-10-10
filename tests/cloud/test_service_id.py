import unittest

from octue.cloud.service_id import convert_service_id_to_pub_sub_form, validate_service_id
from octue.exceptions import InvalidServiceID


class TestConvertServiceIDToPubSubForm(unittest.TestCase):
    def test_convert_service_id_to_pub_sub_form(self):
        """Test that service IDs containing organisations, revision tags, and the services namespace are all converted
        correctly.
        """
        service_ids = (
            ("my-service", "my-service"),
            ("octue/my-service", "octue.my-service"),
            ("octue/my-service:0.1.7", "octue.my-service.0-1-7"),
            ("my-service:3.1.9", "my-service.3-1-9"),
            ("octue.services.octue/my-service:0.1.7", "octue.services.octue.my-service.0-1-7"),
        )

        for uncleaned_service_id, cleaned_service_id in service_ids:
            with self.subTest(uncleaned_service_id=uncleaned_service_id, cleaned_service_id=cleaned_service_id):
                self.assertEqual(convert_service_id_to_pub_sub_form(uncleaned_service_id), cleaned_service_id)


class TestValidateServiceID(unittest.TestCase):
    def test_error_raised_if_service_id_invalid(self):
        """Test that an error is raised if an invalid service ID is given."""
        for service_id in (
            "1.9.4",
            "my-service",
            "my-service:1.9.4",
            "my-org/my-service",
            "-my-org/my-service:1.9.4",
            "my-org/my-service:1.9.4-",
            "my_org/my-service:1.9.4",
            "my-org/my_service:1.9.4",
            "my.org/my.service:1.9.4",
            "my-org/my-service-1.9.4",
            "MY-ORG/my-service:1.9.4",
            "my-org/MY-SERVICE:1.9.4",
        ):
            with self.subTest(service_id=service_id):
                with self.assertRaises(InvalidServiceID):
                    validate_service_id(service_id)

    def test_no_error_raised_if_service_id_invalid(self):
        """Test that no error is raised if a valid service ID is given."""
        for service_id in (
            "my-org/my-service:1.9.4",
            "my-org/my-service:1-9-4",
            "my-org/my-service:1.9.4_",
            "my-org/my-service:1.9.4_beta",
            "my-org/my-service:some_TAG",
        ):
            with self.subTest(service_id=service_id):
                validate_service_id(service_id)
