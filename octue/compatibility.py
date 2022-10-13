import json
import logging
import os


logger = logging.getLogger(__name__)


try:
    with open(os.path.join(os.path.dirname(__file__), "metadata", "version_compatibilities.json")) as f:
        VERSION_COMPATIBILITIES = json.load(f)
except FileNotFoundError:
    logger.warning("Version compatibility data could not be loaded.")
    VERSION_COMPATIBILITIES = {}


def is_compatible(parent_sdk_version, child_sdk_version):
    """Check if two versions of `octue` are compatible according to empirical testing. If there's no information on one
    of the versions, a warning is issued and they're treated as compatible.

    :param str parent_sdk_version: the semantic version of Octue SDK running the parent
    :param str child_sdk_version: the semantic version of Octue SDK running the child
    :return bool:
    """
    if (
        parent_sdk_version not in VERSION_COMPATIBILITIES
        or child_sdk_version not in VERSION_COMPATIBILITIES[parent_sdk_version]
    ):
        logger.warning(
            "No data on compatibility of parent SDK version %s and child SDK version %s.",
            parent_sdk_version,
            child_sdk_version,
        )
        return True

    return VERSION_COMPATIBILITIES[parent_sdk_version][child_sdk_version]


def warn_if_incompatible(parent_sdk_version, child_sdk_version):
    """Log a warning if the parent SDK version isn't compatible with the child SDK version, or if compatibility can't be
    checked due to an absence of version information for one of them.

    :param str|None parent_sdk_version: the version of the Octue SDK running locally / on the local service
    :param str|None child_sdk_version: the version of the Octue SDK running on the remote service
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
