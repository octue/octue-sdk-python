import re

import octue.exceptions


OCTUE_SERVICES_NAMESPACE = "octue.services"
SERVICE_NAMESPACE_AND_NAME_PATTERN = r"([a-z0-9])+(-([a-z0-9])+)*"
REVISION_TAG_PATTERN = r"([A-z0-9_])+([-.]*([A-z0-9_])+)*"
SERVICE_SRUID_PATTERN = (
    rf"^{SERVICE_NAMESPACE_AND_NAME_PATTERN}\/{SERVICE_NAMESPACE_AND_NAME_PATTERN}:{REVISION_TAG_PATTERN}$"
)
COMPILED_SERVICE_SRUID_PATTERN = re.compile(SERVICE_SRUID_PATTERN)


def create_service_id(namespace, name, revision_tag=None):
    """Create a service ID from a namespace, name, and revision tag.

    :param str namespace:
    :param str name:
    :param str revision_tag:
    :return str:
    """
    service_id = f"{namespace}/{name}"

    if revision_tag:
        service_id += f":{revision_tag}"

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


def clean_service_id(service_id):
    """Replace forward slashes and colons with dots in the service ID and, if a service revision is included in the
    service ID, replace any dots in it with dashes.

    :param str service_id: the raw service ID
    :return str: the cleaned service ID.
    """
    if ":" in service_id:
        service_id, service_revision = service_id.split(":")
    else:
        service_revision = None

    service_id = service_id.replace("/", ".")

    if service_revision:
        service_id = service_id + "." + service_revision.replace(".", "-")

    return service_id
