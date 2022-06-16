import pandas


def read_csv_files(files):
    """Read a sequence of CSV files file containing meteorological mast anemometer and wind vane data

    You don't really need to care about this, because your files are unlikely to be in the same form as our
    example csv files. But for the sake of a complete example, we show you how we'd read these in here.

    :param list(octue.resources.Datafile) files: List of the file names to read in and concatenate
    :return pandas.DataFrame: Pandas dataframe containing the imported, uncleaned data
    """
    # This is a simple concatenation. If you have a huge dataset, it's worth getting into working with remote files on
    # the cloud and/or doing this in batches.
    frames = []
    for file in files:
        frames.append(pandas.read_csv(file.local_path))

    return pandas.concat(frames)
