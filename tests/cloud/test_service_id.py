import logging
import os
import unittest
from unittest.mock import patch

from octue.configuration import ServiceConfiguration
from octue.exceptions import InvalidServiceID
from octue.twined.cloud.service_id import (
    DEFAULT_NAMESPACE,
    convert_service_id_to_pub_sub_form,
    create_sruid,
    get_sruid_from_pub_sub_resource_name,
    get_sruid_parts,
    split_service_id,
    validate_sruid,
)
from tests import MOCK_SERVICE_REVISION_TAG


class TestGetSRUIDParts(unittest.TestCase):
    SERVICE_CONFIGURATION = ServiceConfiguration(namespace="octue", name="my-service")

    def test_with_namespace_environment_variable_overriding_service_configuration(self):
        """Test that the service configuration namespace is overridden if the relevant environment variable is present."""
        with patch.dict(os.environ, {"OCTUE_SERVICE_NAMESPACE": "my-org"}):
            with self.assertLogs(level=logging.WARNING) as logging_context:
                namespace, name, revision_tag = get_sruid_parts(self.SERVICE_CONFIGURATION)

        self.assertEqual(
            logging_context.records[0].message,
            "The namespace in the service configuration 'octue' has been overridden by the `OCTUE_SERVICE_NAMESPACE` "
            "environment variable 'my-org'.",
        )

        self.assertEqual(namespace, "my-org")
        self.assertEqual(name, "my-service")
        self.assertIsNone(revision_tag)

    def test_with_name_environment_variable_overriding_service_configuration(self):
        """Test that the service configuration name is overridden if the relevant environment variable is present."""
        with patch.dict(os.environ, {"OCTUE_SERVICE_NAME": "another-service"}):
            with self.assertLogs(level=logging.WARNING) as logging_context:
                namespace, name, revision_tag = get_sruid_parts(self.SERVICE_CONFIGURATION)

        self.assertEqual(
            logging_context.records[0].message,
            "The name in the service configuration 'my-service' has been overridden by the `OCTUE_SERVICE_NAME` "
            "environment variable 'another-service'.",
        )

        self.assertEqual(namespace, "octue")
        self.assertEqual(name, "another-service")
        self.assertIsNone(revision_tag)

    def test_with_revision_tag_environment_variable(self):
        """Test that the service configuration revision tag can be set by the `OCTUE_SERVICE_REVISION_TAG` environment
        variable.
        """
        with patch.dict(os.environ, {"OCTUE_SERVICE_REVISION_TAG": "this-is-a-tag"}):
            with self.assertLogs(level=logging.INFO) as logging_context:
                namespace, name, revision_tag = get_sruid_parts(self.SERVICE_CONFIGURATION)

        self.assertEqual(
            logging_context.records[0].message,
            "Service revision tag 'this-is-a-tag' provided by `OCTUE_SERVICE_REVISION_TAG` environment variable.",
        )

        self.assertEqual(namespace, "octue")
        self.assertEqual(name, "my-service")
        self.assertEqual(revision_tag, "this-is-a-tag")


class TestCreateSRUID(unittest.TestCase):
    def test_error_raised_for_invalid_arguments(self):
        """Test that an error is raised when trying to create an SRUID from invalid components."""
        for namespace, name, revision_tag in (
            ("MY-NAMESPACE", "my-name", "my-tag"),
            ("my-namespace", "MY-NAME", "my-tag"),
            ("my-namespace", "my-name", "@"),
        ):
            with self.subTest(namespace=namespace, name=name, revision_tag=revision_tag):
                with self.assertRaises(InvalidServiceID):
                    create_sruid(namespace, name, revision_tag)

    def test_default(self):
        """Test that a valid SRUID is created when no arguments are provided and that a different SRUID is created each
        time.
        """
        sruid = create_sruid()
        validate_sruid(sruid)
        self.assertTrue(sruid.startswith(DEFAULT_NAMESPACE))
        self.assertNotEqual(sruid, create_sruid())

    def test_with_arguments(self):
        """Test that a valid SRUID is created from valid arguments."""
        sruid = create_sruid("my-namespace", "my-name", "my-tag")
        validate_sruid(sruid)
        self.assertEqual(sruid, "my-namespace/my-name:my-tag")


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
        )

        for service_id, pub_sub_service_id in service_ids:
            with self.subTest(service_id=service_id, pub_sub_service_id=pub_sub_service_id):
                self.assertEqual(convert_service_id_to_pub_sub_form(service_id), pub_sub_service_id)


class TestGetSRUIDFromPubSubResourceName(unittest.TestCase):
    def test_get_sruid_from_pub_sub_resource_name(self):
        """Test that an SRUID can be extracted from a Pub/Sub resource name."""
        sruid = get_sruid_from_pub_sub_resource_name("octue.example-service.0-3-2")
        self.assertEqual(sruid, "octue/example-service:0.3.2")


class TestValidateSRUID(unittest.TestCase):
    def test_error_raised_if_service_id_invalid(self):
        """Test that an error is raised if an invalid SRUID is given."""
        for service_id in (
            "1.9.4",
            "my-service",
            "my-service:",
            "my-service:1.9.4",
            "my-org/my-service",
            "-my-org/my-service:1.9.4",
            "my-org/my-service:1.9.4-",
            "my_org/my-service:1.9.4",
            "my-org/my_service:1.9.4",
            "my.org/my-service:1.9.4",
            "my-org/my.service:1.9.4",
            "my-org/my-service-1.9.4",
            "MY-ORG/my-service:1.9.4",
            "my-org/MY-SERVICE:1.9.4",
            "my-org/MY-SERVICE:@",
            f"my-org/my-service:{'1' * 129}",
            "/my-service",
            "/my-service:",
        ):
            with self.subTest(service_id=service_id):
                with self.assertRaises(InvalidServiceID):
                    validate_sruid(sruid=service_id)

    def test_no_error_raised_if_sruid_valid(self):
        """Test that no error is raised if a valid SRUID is given."""
        for service_id in (
            "my-org/my-service:1.9.4",
            "my-org1/my-service:1.9.4",
            "my-org/my-service9:1.9.4",
            "my-org/my-service:1-9-4",
            "my-org/my-service:1.9.4_",
            "my-org/my-service:1.9.4_beta",
            "my-org/my-service:some_TAG",
        ):
            with self.subTest(service_id=service_id):
                validate_sruid(sruid=service_id)

    def test_error_raised_if_not_all_sruid_components_provided(self):
        """Test that an error is raised if, when not providing the `service_id` argument, not all of the `namespace,
        `name`, and `revision_tag` arguments are provided.
        """
        for namespace, name, revision_tag in (
            ("my-org", "my-service", None),
            ("my-org", None, "1.2.3"),
            (None, "my-service", "1.2.3"),
        ):
            with self.subTest(namespace=namespace, name=name, revision_tag=revision_tag):
                with self.assertRaises(ValueError):
                    validate_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

    def test_error_raised_if_sruid_components_invalid(self):
        """Test that an error is raised if any of the SRUID components are individually invalid."""
        for namespace, name, revision_tag in (
            ("-my-org", "my-service", "1.9.4"),
            ("my-org", "-my-service", "1.9.4"),
            ("my-org", "my-service", "-1.9.4"),
            ("my_org", "my-service", "1.9.4"),
            ("my-org", "my_service", "1.9.4"),
            ("my.org", "my-service", "1.9.4"),
            ("my-org", "my.service", "1.9.4"),
            ("MY-ORG", "my-service", "1.9.4"),
            ("my-org", "MY-SERVICE", "1.9.4"),
            ("my-org", "my-service", "@"),
            ("my-org", "my-service", f"{'1' * 129}"),
        ):
            with self.subTest(namespace=namespace, name=name, revision_tag=revision_tag):
                with self.assertRaises(InvalidServiceID):
                    validate_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

    def test_no_error_raised_if_sruid_components_valid(self):
        """Test that no error is raised if all components of the SRUID are valid."""
        for namespace, name, revision_tag in (
            ("my-org", "my-service", "1.9.4"),
            ("my-org1", "my-service", "1.9.4"),
            ("my-org", "my-service9", "1.9.4"),
            ("my-org", "my-service", "1-9-4"),
            ("my-org", "my-service", "1.9.4_"),
            ("my-org", "my-service", "1.9.4_beta"),
            ("my-org", "my-service", "some_TAG"),
        ):
            with self.subTest(namespace=namespace, name=name, revision_tag=revision_tag):
                validate_sruid(namespace=namespace, name=name, revision_tag=revision_tag)


class TestSplitServiceID(unittest.TestCase):
    def test_split_sruid(self):
        """Test that a valid SRUID can be split into its namespace, name, and revision tag."""
        namespace, name, revision_tag = split_service_id(f"octue/my-service:{MOCK_SERVICE_REVISION_TAG}")
        self.assertEqual(namespace, "octue")
        self.assertEqual(name, "my-service")
        self.assertEqual(revision_tag, MOCK_SERVICE_REVISION_TAG)

    def test_split_service_id(self):
        """Test that a service ID without a revision tag can be split into its namespace and name."""
        namespace, name, revision_tag = split_service_id("octue/my-service")
        self.assertEqual(namespace, "octue")
        self.assertEqual(name, "my-service")
        self.assertIsNone(revision_tag)
