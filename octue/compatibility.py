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
            "No data on compatibility of parent SDK version %s and child SDK version %s.",
            parent_version,
            child_version,
        )
        return True

    return VERSION_COMPATIBILITIES[parent_version][child_version]


def warn_if_incompatible(parent_sdk_version, child_sdk_version):
    """Log a warning if the local SDK version isn't compatible with the remote version, or if compatibility can't be
    checked due to an absence of remote version information.

    :param str local_sdk_version: the version of the Octue SDK running locally / on the local service
    :param str|None remote_sdk_version: the version of the Octue SDK running on the remote service
    :return None:
    """
    if not parent_sdk_version:
        missing_service_version_information = "parent"
    elif not child_sdk_version:
        missing_service_version_information = "child"
    else:
        missing_service_version_information = None

    if missing_service_version_information:
        logger.warning(
            "The %s couldn't be checked for compatibility with this service because its Octue SDK version wasn't "
            "provided. Please update it to the latest Octue SDK version.",
            missing_service_version_information,
        )
        return

    if not is_compatible(parent_sdk_version, child_sdk_version):
        logger.warning(
            "The parent's Octue SDK version %s is incompatible with the child's version %s. Please update either or "
            "both to the latest version.",
            parent_sdk_version,
            child_sdk_version,
        )
