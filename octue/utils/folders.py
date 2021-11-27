import os

from octue.definitions import OUTPUT_STRANDS, STRAND_FILENAME_MAP


def get_file_name_from_strand(strand, path):
    """Where values or manifest are contained in a local file, assemble that filename.

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
