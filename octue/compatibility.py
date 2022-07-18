from packaging.version import Version


LATEST_KNOWN_INCOMPATIBLE_VERSIONS = {"0.27.1": Version("0.24.1")}


def is_compatible(version_0, version_1):
    """Check if two versions of `octue` are compatible. The versions are compatible if:
    - The versions are the same
    - The lower version is above the latest known incompatible version for the higher version

    The versions are treated as incompatible if no data is available on the higher version's compatibility.

    :param str version_0:
    :param str version_1:
    :return bool:
    """
    if version_0 == version_1:
        return True

    lower_version, higher_version = sorted([Version(version_0), Version(version_1)])

    latest_incompatible_version = LATEST_KNOWN_INCOMPATIBLE_VERSIONS.get(str(higher_version))

    if not latest_incompatible_version:
        return False

    return lower_version > latest_incompatible_version
