import json
import logging
import os


logger = logging.getLogger(__name__)


with open(os.path.join(os.path.dirname(__file__), "metadata", "version_compatibilities.json")) as f:
    VERSION_COMPATIBILITIES = json.load(f)


def is_compatible(parent_version, child_version):
    """Check if two versions of `octue` are compatible according to empirical testing. The versions are treated as
    compatible if there's no information on one of the versions, but a warning is also issued.

    :param str parent_version: the semantic version of the parent
    :param str child_version: the semantic version of the child
    :return bool:
    """
    if parent_version not in VERSION_COMPATIBILITIES or child_version not in VERSION_COMPATIBILITIES[parent_version]:
        logger.warning(
            "No data on compatibility of parent version %s and child version %s.",
            parent_version,
            child_version,
        )
        return True

    return VERSION_COMPATIBILITIES[parent_version][child_version]


def warn_if_incompatible(local_sdk_version, remote_sdk_version, perspective):
    """Log a warning if the local SDK version isn't compatible with the remote version, or if compatibility can't be
    checked due to an absence of remote version information.

    :param str local_sdk_version: the version of the Octue SDK running locally / on the local service
    :param str|None remote_sdk_version: the version of the Octue SDK running on the remote service
    :param str perspective: the perspective from which the warnings will be issued; must be one of the strings 'child' or 'parent'
    :raise ValueError: if the `perspective` argument isn't one of 'child' or 'parent'
    :return None:
    """
    if perspective not in {"child", "parent"}:
        raise ValueError(
            f"The `perspective` argument must take the value of either 'child' or 'parent', not {perspective!r}."
        )

    if perspective == "child":
        remote_service_type = "parent"
    elif perspective == "parent":
        remote_service_type = "child"

    local_service_type = perspective

    if not remote_sdk_version:
        logger.warning(
            "The %s couldn't be checked for compatibility with this service because it didn't send its Octue SDK "
            "version with its messages. Please update it to the latest Octue SDK version.",
            remote_service_type,
        )
        return

    if not is_compatible(local_sdk_version, remote_sdk_version):
        logger.warning(
            "The %s's Octue SDK version %s is incompatible with the %s's version %s. Please update either or both to "
            "the latest version.",
            local_service_type,
            local_sdk_version,
            remote_service_type,
            remote_sdk_version,
        )
