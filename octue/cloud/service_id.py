import logging
import os
import re

import coolname

import octue.exceptions


logger = logging.getLogger(__name__)


OCTUE_SERVICES_NAMESPACE = "octue.services"

SERVICE_NAMESPACE_AND_NAME_PATTERN = r"([a-z0-9])+(-([a-z0-9])+)*"
COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN = re.compile(SERVICE_NAMESPACE_AND_NAME_PATTERN)

REVISION_TAG_PATTERN = r"([A-z0-9_])+([-.]*([A-z0-9_])+)*"
COMPILED_REVISION_TAG_PATTERN = re.compile(REVISION_TAG_PATTERN)

SERVICE_SRUID_PATTERN = (
    rf"^{SERVICE_NAMESPACE_AND_NAME_PATTERN}\/{SERVICE_NAMESPACE_AND_NAME_PATTERN}:{REVISION_TAG_PATTERN}$"
)

COMPILED_SERVICE_SRUID_PATTERN = re.compile(SERVICE_SRUID_PATTERN)


def get_service_sruid_parts(service_configuration):
    """Get the namespace, name, and revision tag for the service from either the service environment variables or the
    service configuration (in that order of precedence). The service revision tag is `None` if it's not provided in the
    `OCTUE_SERVICE_REVISION_TAG` environment variable as it can't be specified in the service configuration.

    :param octue.configuration.ServiceConfiguration service_configuration: the service configuration to get the service namespace and name from
    :return (str, str, str|None):
    """
    service_namespace = os.environ.get("OCTUE_SERVICE_NAMESPACE")
    service_name = os.environ.get("OCTUE_SERVICE_NAME")
    service_revision_tag = os.environ.get("OCTUE_SERVICE_REVISION_TAG")

    if service_namespace:
        logger.warning(
            "The namespace in the service configuration %r has been overridden by the `OCTUE_SERVICE_NAMESPACE` "
            "environment variable %r.",
            service_configuration.namespace,
            service_namespace,
        )
    else:
        service_namespace = service_configuration.namespace

    if service_name:
        logger.warning(
            "The name in the service configuration %r has been overridden by the `OCTUE_SERVICE_NAME` environment "
            "variable %r.",
            service_configuration.name,
            service_name,
        )
    else:
        service_name = service_configuration.name

    if service_revision_tag:
        logger.info(
            "Service revision tag %r provided by `OCTUE_SERVICE_REVISION_TAG` environment variable.",
            service_revision_tag,
        )

    return service_namespace, service_name, service_revision_tag


def create_service_sruid(namespace, name, revision_tag=None):
    """Create and validate a service revision unique identifier (SRUID) from a namespace, name, and revision tag. If no
    revision tag is given, a "cool name" revision tag is generated.

    :param str namespace: the name of the group to which the service belongs
    :param str name: the name of the service
    :param str|None revision_tag: a tag that uniquely identifies a particular revision of the service
    :raise octue.exceptions.InvalidServiceID: if any of the namespace, name, or revision tag are invalid
    :return str: the valid SRUID comprising the namespace, name, and revision tag
    """
    revision_tag = revision_tag or coolname.generate_slug(2)
    validate_service_sruid(namespace=namespace, name=name, revision_tag=revision_tag)
    return f"{namespace}/{name}:{revision_tag}"


def validate_service_sruid(service_sruid=None, namespace=None, name=None, revision_tag=None):
    """Raise an error if the service revision unique identifier (SRUID) or its components don't meet the required
    patterns. Either the `service_id` or all of the `namespace`, `name`, and `revision_tag` arguments must be given.

    :param str|None service_sruid: the service SRUID to validate
    :param str|None namespace: the namespace of a service to validate
    :param str|None name: the name of a service to validate
    :param str|None revision_tag: the revision tag of a service to validate
    :raise octue.exceptions.InvalidServiceID: if the service SRUID or any of its components are invalid
    :return None:
    """
    if service_sruid:
        if not COMPILED_SERVICE_SRUID_PATTERN.fullmatch(service_sruid):
            raise octue.exceptions.InvalidServiceID(
                f"{service_sruid!r} is not a valid service revision unique identifier (SRUID). It must be in the format "
                f"<namespace>/<name>:<revision_tag>. The namespace and name must be lower kebab case (i.e. only "
                f"contain the letters [a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen. The "
                f"revision tag can contain lowercase and uppercase letters, numbers, underscores, periods, and "
                f"hyphens, but can't start with a period or a dash. It can contain a maximum of 128 characters. These "
                f"requirements are the same as the Docker tag format."
            )

        revision_tag = service_sruid.split(":")[-1]

        if len(revision_tag) > 128:
            raise octue.exceptions.InvalidServiceID(
                f"The maximum length for a revision tag is 128 characters. Received {revision_tag!r}."
            )

        return

    if not (namespace and name and revision_tag):
        raise ValueError(
            "If not providing the `service_id` argument for SRUID validation, all of the `namespace`, `name`, and "
            "`revision_tag` arguments must be provided instead."
        )

    if not COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN.fullmatch(namespace):
        raise octue.exceptions.InvalidServiceID(
            f"{namespace!r} is not a valid namespace for a service. It must be lower kebab case (i.e. only contain "
            "the letters [a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen."
        )

    if not COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN.fullmatch(name):
        raise octue.exceptions.InvalidServiceID(
            f"{name!r} is not a valid name for a service. It must be lower kebab case (i.e. only contain the letters "
            f"[a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen."
        )

    if len(revision_tag) > 128:
        raise octue.exceptions.InvalidServiceID(
            f"The maximum length for a revision tag is 128 characters. Received {revision_tag!r}."
        )

    if not COMPILED_REVISION_TAG_PATTERN.fullmatch(revision_tag):
        raise octue.exceptions.InvalidServiceID(
            f"{revision_tag!r} is not a valid revision tag for a service. It can contain lowercase and uppercase "
            "letters, numbers, underscores, periods, and hyphens, but can't start with a period or a dash. It can "
            "contain a maximum of 128 characters. These requirements are the same as the Docker tag format."
        )


def convert_service_id_to_pub_sub_form(service_id):
    """Convert the service ID to the form required for use in Google Pub/Sub topic and subscription paths. This is done
    by replacing forward slashes and colons with periods and, if a service revision tag is included, replacing any
    periods in it with dashes.

    :param str service_id: a service ID or service revision unique identifier (SRUID)
    :return str: the service ID in Google Pub/Sub form
    """
    if ":" in service_id:
        service_id, service_revision_tag = service_id.split(":")
    else:
        service_revision_tag = None

    service_id = service_id.replace("/", ".")

    if service_revision_tag:
        service_id = service_id + "." + service_revision_tag.replace(".", "-")

    return service_id
