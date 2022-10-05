import re

import octue.exceptions


SERVICE_NAMESPACE_AND_NAME_PATTERN = r"([a-z0-9])+(-([a-z0-9])+)*"
REVISION_TAG_PATTERN = r"([A-z0-9_])+([-.]*([A-z0-9_])+)*"

SERVICE_SRUID_PATTERN = (
    rf"^{SERVICE_NAMESPACE_AND_NAME_PATTERN}\/{SERVICE_NAMESPACE_AND_NAME_PATTERN}:{REVISION_TAG_PATTERN}$"
)

COMPILED_SERVICE_SRUID_PATTERN = re.compile(SERVICE_SRUID_PATTERN)


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
