import json
import os
import subprocess


VERSION_COMPATIBILITY_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "octue",
    "metadata",
    "version_compatibilities.json",
)

with open(VERSION_COMPATIBILITY_DATA_PATH) as f:
    VERSION_COMPATIBILITY_DATA = json.load(f)

CURRENT_VERSION = subprocess.run(["poetry", "version", "-s"], capture_output=True).stdout.decode().strip()

compatibility_not_checked_error = ValueError(
    f"The current version {CURRENT_VERSION!r} has not been checked for compatibility with previous versions."
)

if CURRENT_VERSION not in VERSION_COMPATIBILITY_DATA:
    raise compatibility_not_checked_error

for version in VERSION_COMPATIBILITY_DATA:
    if CURRENT_VERSION not in VERSION_COMPATIBILITY_DATA[version]:
        raise compatibility_not_checked_error
