import logging
import os
import re

import coolname

import octue.exceptions


logger = logging.getLogger(__name__)


OCTUE_SERVICES_NAMESPACE = "octue.services"
SERVICE_NAMESPACE_AND_NAME_PATTERN = r"([a-z0-9])+(-([a-z0-9])+)*"
REVISION_TAG_PATTERN = r"([A-z0-9_])+([-.]*([A-z0-9_])+)*"

SERVICE_SRUID_PATTERN = (
    rf"^{SERVICE_NAMESPACE_AND_NAME_PATTERN}\/{SERVICE_NAMESPACE_AND_NAME_PATTERN}:{REVISION_TAG_PATTERN}$"
)

COMPILED_SERVICE_SRUID_PATTERN = re.compile(SERVICE_SRUID_PATTERN)


def get_service_sruid_parts(service_configuration):
    """Get the namespace and name for the service from either the service environment variables or the service
    configuration (in that order of precedence). The service revision tag is included if it's provided in the
    `OCTUE_SERVICE_TAG` environment variable; otherwise, it's `None`.

    :param octue.configuration.ServiceConfiguration service_configuration:
    :return (str, str, str|None):
    """
    service_namespace = os.environ.get("OCTUE_SERVICE_NAMESPACE")
    service_name = os.environ.get("OCTUE_SERVICE_NAME")
    service_tag = os.environ.get("OCTUE_SERVICE_TAG")

    if service_namespace:
        logger.warning(
            "The namespace in the service configuration %r has been overridden by the `OCTUE_SERVICE_NAMESPACE` "
            "environment variable %r.",
            service_configuration.namespace,
            service_namespace,
        )
    else:
        service_namespace = service_configuration.name

    if service_name:
        logger.warning(
            "The name in the service configuration %r has been overridden by the `OCTUE_SERVICE_NAME` environment "
            "variable %r.",
            service_configuration.name,
            service_name,
        )
    else:
        service_name = service_configuration.name

    return service_namespace, service_name, service_tag


def create_service_id(namespace, name, revision_tag=None):
    """Create a service ID from a namespace, name, and revision tag. The resultant ID is validated before returning. If
    no revision tag is given, a "cool name" revision tag is generated.

    :param str namespace:
    :param str name:
    :param str|None revision_tag:
    :raise octue.exceptions.InvalidServiceID: if the service ID is invalid
    :return str:
    """
    revision_tag = revision_tag or coolname.generate_slug(2)
    service_id = f"{namespace}/{name}:{revision_tag}"
    validate_service_id(service_id)
    return service_id


def validate_service_id(service_id):
    """Raise an error if the service ID doesn't meet the defined patterns.

    :param str service_id:
    :raise octue.exceptions.InvalidServiceID: if the service ID is invalid.
    :return None:
    """
    if not COMPILED_SERVICE_SRUID_PATTERN.match(service_id):
        raise octue.exceptions.InvalidServiceID(
            f"{service_id!r} is not a valid service ID. Make sure it meets this regex: {SERVICE_SRUID_PATTERN!r}."
        )


def convert_service_id_to_pub_sub_form(service_id):
    """Convert the service ID to the form required for use in Google Pub/Sub topic and subscription paths. This is done
    by replacing forward slashes and colons with periods and, if a service revision is included, replacing any periods
    in it with dashes.

    :param str service_id: the user-friendly service ID
    :return str: the service ID in Google Pub/Sub form
    """
    if ":" in service_id:
        service_id, service_revision = service_id.split(":")
    else:
        service_revision = None

    service_id = service_id.replace("/", ".")

    if service_revision:
        service_id = service_id + "." + service_revision.replace(".", "-")

    return service_id
