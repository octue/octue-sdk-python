from dateparser import parse


def read_dat_file(file):
    """Read a dat file containing meteorological mast metadata.

    You don't really need to care about this, because your files are unlikely to be in the same form as our
    example dat files. You'll need to build your own file readers and test them (or use open-source libraries to do
    the reading).

    So this is only here to make the example work. Although you could use this as a case study of how annoying it is to
    parse custom formatted data files - even "simple" ones!

    :param octue.resources.Datafile file: File from the dataset that contains metadata
    :return dict: Dictionary of available metadata
    """
    # Read the file.
    with file.open("r") as text_file:
        lines = text_file.readlines()

    # Parse the metadata.
    metadata = {}

    for line in lines:
        # In the proprietary example dat file, tabs separate parameters from their value, so if a line contains a tab,
        # import that parameter-value pair.
        if ":" in line:
            param = line.split(":", 1)
            key = param[0].strip().lower()
            value = param[1].strip()
            value = parse(value) if key == "date" else float(value)
            metadata[key] = value

    # Make sure that a `date` field is in the metadata, as we'll need it later
    if "date" not in metadata.keys():
        raise ValueError("No DATE field in the metadata file (required)")

    return metadata
