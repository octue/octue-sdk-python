import os

from octue import exceptions
from .isfolder import isfolder


FOLDERS = (
    "configuration",
    "input",
    "log",
    "tmp",
    "output",
)


def from_path(path_hints, folders=FOLDERS):
    """ NOT IMPLEMENTED YET - Helper to find paths to individual configurations from hints
    TODO Fix this
    """
    # Set paths
    paths = dict()
    if isinstance(path_hints, str):
        if not os.path.isdir(path_hints):
            raise exceptions.FolderNotFoundException(f"Specified data folder '{path_hints}' not present")

        paths = dict([(folder, os.path.join(path_hints, folder)) for folder in folders])

    else:
        if (
            not isinstance(paths, dict)
            or (len(paths.keys()) != len(folders))
            or not all([k in folders for k in paths.keys()])
        ):
            raise exceptions.InvalidInputException(
                f"Input 'paths' should be a dict containing directory paths with the following keys: {folders}"
            )

    # Ensure paths exist on disc??
    for folder in FOLDERS:
        isfolder(paths[folder], make_if_absent=True)
