import logging
import os
import re

import coolname
import requests

import octue.exceptions


logger = logging.getLogger(__name__)


OCTUE_SERVICES_NAMESPACE = "octue.services"

SERVICE_NAMESPACE_AND_NAME_PATTERN = r"([a-z0-9])+(-([a-z0-9])+)*"
COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN = re.compile(SERVICE_NAMESPACE_AND_NAME_PATTERN)

REVISION_TAG_PATTERN = r"([A-z0-9_])+([-.]*([A-z0-9_])+)*"
COMPILED_REVISION_TAG_PATTERN = re.compile(REVISION_TAG_PATTERN)

SRUID_PATTERN = rf"^{SERVICE_NAMESPACE_AND_NAME_PATTERN}\/{SERVICE_NAMESPACE_AND_NAME_PATTERN}:{REVISION_TAG_PATTERN}$"
COMPILED_SRUID_PATTERN = re.compile(SRUID_PATTERN)


def get_sruid_parts(service_configuration):
    """Get the namespace, name, and revision tag for the service from either the service environment variables or the
    service configuration (in that order of precedence). The service revision tag is `None` if it's not provided in the
    `OCTUE_SERVICE_REVISION_TAG` environment variable as it can't be specified in the service configuration.

    :param octue.configuration.ServiceConfiguration service_configuration: the service configuration to get the service namespace and name from
    :return (str, str, str|None):
    """
    service_namespace = os.environ.get("OCTUE_SERVICE_NAMESPACE")
    service_name = os.environ.get("OCTUE_SERVICE_NAME")
    service_revision_tag = os.environ.get("OCTUE_SERVICE_REVISION_TAG")

    if service_namespace and service_namespace != service_configuration.namespace:
        logger.warning(
            "The namespace in the service configuration %r has been overridden by the `OCTUE_SERVICE_NAMESPACE` "
            "environment variable %r.",
            service_configuration.namespace,
            service_namespace,
        )
    else:
        service_namespace = service_configuration.namespace

    if service_name and service_name != service_configuration.name:
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


def create_sruid(namespace, name, revision_tag=None):
    """Create and validate a service revision unique identifier (SRUID) from a namespace, name, and revision tag. If no
    revision tag is given, a "cool name" revision tag is generated.

    :param str namespace: the name of the group to which the service belongs
    :param str name: the name of the service
    :param str|None revision_tag: a tag that uniquely identifies a particular revision of the service
    :raise octue.exceptions.InvalidServiceID: if any of the namespace, name, or revision tag are invalid
    :return str: the valid SRUID comprising the namespace, name, and revision tag
    """
    revision_tag = revision_tag or coolname.generate_slug(2)
    validate_sruid(namespace=namespace, name=name, revision_tag=revision_tag)
    return f"{namespace}/{name}:{revision_tag}"


def validate_sruid(sruid=None, namespace=None, name=None, revision_tag=None):
    """Raise an error if the service revision unique identifier (SRUID) or its components don't meet the required
    patterns. Either the `service_id` or all of the `namespace`, `name`, and `revision_tag` arguments must be given.

    :param str|None sruid: the service SRUID to validate
    :param str|None namespace: the namespace of a service to validate
    :param str|None name: the name of a service to validate
    :param str|None revision_tag: the revision tag of a service to validate
    :raise octue.exceptions.InvalidServiceID: if the service SRUID or any of its components are invalid
    :return None:
    """
    if sruid:
        if not COMPILED_SRUID_PATTERN.fullmatch(sruid):
            raise octue.exceptions.InvalidServiceID(
                f"{sruid!r} is not a valid service revision unique identifier (SRUID). It must be in the format "
                f"<namespace>/<name>:<revision_tag>. The namespace and name must be lower kebab case (i.e. only "
                f"contain the letters [a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen. The "
                f"revision tag can contain lowercase and uppercase letters, numbers, underscores, periods, and "
                f"hyphens, but can't start with a period or a dash. It can contain a maximum of 128 characters. These "
                f"requirements are the same as the Docker tag format."
            )

        revision_tag = sruid.split(":")[-1]

        if len(revision_tag) > 128:
            raise octue.exceptions.InvalidServiceID(
                f"The maximum length for a revision tag is 128 characters. Received {revision_tag!r}."
            )

        return

    if any((namespace is None, name is None, revision_tag is None)):
        raise ValueError(
            "If not providing the `service_id` argument for SRUID validation, all of the `namespace`, `name`, and "
            "`revision_tag` arguments must be provided instead."
        )

    validate_namespace(namespace)
    validate_name(name)

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


def validate_service_id(service_id=None, namespace=None, name=None):
    """Raise an error if the service ID or its components don't meet the required patterns. Either the `service_id` or
    both the `namespace` and `name` arguments must be given.

    :param str|None service_id: the service ID to validate
    :param str|None namespace: the namespace of a service to validate
    :param str|None name: the name of a service to validate
    :raise octue.exceptions.InvalidServiceID: if the service ID or any of its components are invalid
    :return None:
    """
    if service_id:
        if not COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN.fullmatch(service_id):
            raise octue.exceptions.InvalidServiceID(
                f"{service_id!r} is not a valid service ID. It must be in the format <namespace>/<name>. The namespace "
                "and name must be lower kebab case (i.e. only contain the letters [a-z], numbers [0-9], and hyphens [-]"
                ") and not begin or end with a hyphen."
            )

        return

    if any((namespace is None, name is None)):
        raise ValueError(
            "If not providing the `service_id` argument for service ID validation, both the `namespace` and `name` "
            "arguments must be provided instead."
        )

    validate_namespace(namespace)
    validate_name(name)


def validate_namespace(namespace):
    """Raise an error if the service namespace doesn't meet the required patterns.

    :param str|None namespace: the namespace to validate
    :return None:
    """
    if not COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN.fullmatch(namespace):
        raise octue.exceptions.InvalidServiceID(
            f"{namespace!r} is not a valid namespace for a service. It must be lower kebab case (i.e. only contain "
            "the letters [a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen."
        )


def validate_name(name):
    """Raise an error if the service name doesn't meet the required patterns.

    :param str|None name: the name to validate
    :return None:
    """
    if not COMPILED_SERVICE_NAMESPACE_AND_NAME_PATTERN.fullmatch(name):
        raise octue.exceptions.InvalidServiceID(
            f"{name!r} is not a valid name for a service. It must be lower kebab case (i.e. only contain the letters "
            f"[a-z], numbers [0-9], and hyphens [-]) and not begin or end with a hyphen."
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


def get_sruid_from_pub_sub_resource_name(name):
    """Get the SRUID from a Google Pub/Sub topic or subscription name. Note that any hyphens in the revision tag will
    be replaced with periods.

    :param str name: the name of the topic or subscription
    :return str: the SRUID of the service revision the topic or subscription is related to
    """
    _, _, namespace, name, revision_tag, *_ = name.split(".")
    return f"{namespace}/{name}:{revision_tag.replace('-', '.')}"


def split_service_id(service_id, require_revision_tag=False):
    """Split an SRUID or service ID into its namespace, name, and, if present, its revision tag. The split parts are
    validated before being returned.

    :param str service_id: the SRUID or service ID to split
    :param bool require_revision_tag: if `True`, require the service ID to include a revision tag (i.e. require the service ID to be an SRUID)
    :raise octue.exceptions.InvalidServiceID: if any of the namespace, name, or revision tag are invalid
    :return tuple(str, str, str|None): the namespace, name, and revision tag
    """
    namespace, name_and_revision_tag = service_id.split("/")

    try:
        name, revision_tag = name_and_revision_tag.split(":")
    except ValueError:
        name = name_and_revision_tag
        revision_tag = None

    if revision_tag is None and not require_revision_tag:
        validate_service_id(namespace=namespace, name=name)
    else:
        validate_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

    return namespace, name, revision_tag


def get_default_sruid(namespace, name, service_registries):
    """Get the SRUID of the default revision of the service `<namespace>/<name>` if it exists in one of the specified
    service registries. The registries should be provided in priority order so that, if more than one registry contains
    a matching service, the revision that's returned is taken from the highest priority (first) registry.

    :param str namespace: the namespace of the service
    :param str name: the name of the service
    :param iter(dict) service_registries: the registries to look for the service in; the registries should be in priority order in case more than one has a service with the given namespace and name
    :raise octue.exceptions.ServiceNotFound: if a revision can't be found for the service in the service registries
    :return str: the SRUID of the default revision of the service
    """
    service_id = f"{namespace}/{name}"

    for registry in service_registries:
        response = requests.get(f"{registry['endpoint']}/{service_id}")

        if response.ok:
            revision_tag = response.json()["revision_tag"]
            logger.info("Found service revision '%s:%s' in %r registry.", service_id, revision_tag, registry["name"])
            return create_sruid(namespace=namespace, name=name, revision_tag=revision_tag)

    raise octue.exceptions.ServiceNotFound(
        f"No revisions for the service {service_id!r} were found in any of the specified service registries: "
        f"{service_registries!r}"
    )


def raise_if_revision_not_registered(sruid, service_registries):
    """Raise an error if the service revision isn't registered in the given service registries.

    :param str sruid: the SRUID of the service revision
    :param iter(dict) service_registries: the registries to look for the service revision in
    :raise octue.exceptions.ServiceNotFound: if the service revision isn't registered in any of the service registries
    :return None:
    """
    namespace, name, revision_tag = split_service_id(sruid, require_revision_tag=True)

    for registry in service_registries:
        response = requests.get(f"{registry['endpoint']}/{namespace}/{name}?revision_tag={revision_tag}")

        if response.ok:
            logger.info("Found service revision %r in %r registry.", sruid, registry["name"])
            return

    raise octue.exceptions.ServiceNotFound(
        f"Service revision {sruid!r} was not found in any of the specified service registries: {service_registries!r}"
    )
