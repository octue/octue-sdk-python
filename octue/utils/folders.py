import os

from octue import exceptions
from octue.definitions import OUTPUT_STRANDS, STRAND_FILENAME_MAP
from twined.twine import CHILDREN_STRANDS, CREDENTIAL_STRANDS, MANIFEST_STRANDS, MONITOR_STRANDS, SCHEMA_STRANDS
from .isfolder import isfolder


ALL_STRANDS = (
    *SCHEMA_STRANDS,
    *MANIFEST_STRANDS,
    *CREDENTIAL_STRANDS,
    *CHILDREN_STRANDS,
    *MONITOR_STRANDS,
)


FOLDERS = (
    "configuration",
    "input",
    "tmp",
    "output",
)


def get_file_name_from_strand(strand, path):
    """ Where values or manifest are contained in a local file, assemble that filename.

    For output directories, the directory will be made if it doesn't exist. This is not true for input directories
    for which validation of their presence is handled elsewhere.

    :param strand: The name of the strand
    :type strand: basestring

    :param path: The directory where the file is / will be saved
    :type path: path-like

    :return: A file name for the strand
    :rtype: path-like
    """

    if strand in OUTPUT_STRANDS:
        os.makedirs(path, exist_ok=True)

    return os.path.join(path, STRAND_FILENAME_MAP[strand])


def from_path(path_hints, folders=FOLDERS):
    """ NOT IMPLEMENTED YET - Helper to find paths to individual configurations from hints
    TODO Fix this
    """
    # Set paths
    paths = dict()
    if isinstance(path_hints, str):
        if not os.path.isdir(path_hints):
            raise exceptions.FolderNotFoundException(f"Specified data folder '{path_hints}' not present")

        paths = {folder: os.path.join(path_hints, folder) for folder in folders}

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
