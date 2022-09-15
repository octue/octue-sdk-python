from packaging.version import Version


VERSION_0_24_1 = Version("0.24.1")


LATEST_KNOWN_INCOMPATIBLE_VERSIONS = {
    "0.35.1": VERSION_0_24_1,
    "0.35.0": VERSION_0_24_1,
    "0.34.1": VERSION_0_24_1,
    "0.34.0": VERSION_0_24_1,
    "0.33.0": VERSION_0_24_1,
    "0.32.0": VERSION_0_24_1,
    "0.31.0": VERSION_0_24_1,
    "0.30.0": VERSION_0_24_1,
    "0.29.11": VERSION_0_24_1,
    "0.29.10": VERSION_0_24_1,
    "0.29.9": VERSION_0_24_1,
    "0.29.8": VERSION_0_24_1,
    "0.29.7": VERSION_0_24_1,
    "0.29.6": VERSION_0_24_1,
    "0.29.5": VERSION_0_24_1,
    "0.29.4": VERSION_0_24_1,
    "0.29.3": VERSION_0_24_1,
    "0.29.2": VERSION_0_24_1,
    "0.29.1": VERSION_0_24_1,
    "0.29.0": VERSION_0_24_1,
    "0.28.2": VERSION_0_24_1,
    "0.28.1": VERSION_0_24_1,
    "0.28.0": VERSION_0_24_1,
    "0.27.3": VERSION_0_24_1,
    "0.27.2": VERSION_0_24_1,
    "0.27.1": VERSION_0_24_1,
}


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
