import logging

from cleaner import clean, read_csv_files, read_dat_file

from octue.resources import Datafile


logger = logging.getLogger(__name__)


def run(analysis, *args, **kwargs):
    """Read a time series of files from a dataset, clean them, and write a new, cleaned, dataset.

    See the "fractal" template for an introduction to the analysis object and the purpose of this 'run' function.

    Here, let's create an example application designed to clean up CSV data files produced by an instrument, in this
    case a meteorological mast.

    The aim of this example is to teach you how to use input and output file manifests in an app - so what we'll do is:
    - Use the input manifest to read a sequence of files
    - Perform a simple transformation on some of the data (as if we were doing a data cleaning process)
    - Create new files containing the cleaned data
    - Add them to the output manifest
    """
    # You can use the attached logger to record debug statements, general information, warnings or errors
    logger.info("Starting clean up of files in %s", analysis.input_manifest)

    # Get the configuration value for our time averaging window (or if not present, use the default specified in
    # the twine)
    time_window = (analysis.configuration_values.get("time_window", 600),)
    logger.info("Averaging window set to %ss", time_window)

    # Get the input dataset which will be read in
    input_dataset = analysis.input_manifest.get_dataset("raw_met_mast_data")

    # There are two types of files in the dataset. Metadata file(s), saved daily, and measurement files (saved hourly).
    # Because a manifest has been created, we're able to get this data out easily with the dataset filtering
    # capabilities. Let's get the metadata and the timeseries files, whilst showing off a couple of the filters.
    #
    # See the Dataset class help for more.
    metadata_file = input_dataset.get_file_by_label("meta")

    timeseries_files = input_dataset.files.filter(labels__contains="timeseries").order_by(
        "tags__sequence", check_start_value=0, check_constant_increment=1
    )
    #
    # We used these because they're special helpers - in this case ensuring that there's only one metadata file and
    # ensuring that the timeseries files come in a strictly ordered sequence.
    #
    # We could also have picked up one or more files using general filters, like so:
    #    metadata_files = input_dataset.get_files("name__icontains", filter_value="meta")
    #
    # There's generally a few ways to do it. Choose one which is likely to be most consistent - for example if your
    # filenames might be subject to change, but you have better control over the labels, rely on those.

    # At this point it's over to you, to do whatever you want with the contents of these files.
    # For this example app, we will:
    #
    #       Use a custom function to read in the strange metadata file that came with the dataset
    metadata = read_dat_file(metadata_file)
    #
    #       Read the sequence of CSV files and concatenate into a pandas dataframe (like a table)
    data = read_csv_files(timeseries_files)
    #
    #       Clean the timeseries data up
    data = clean(data, metadata["date"])

    # The twine specifies an output dataset, so it's already been created for you (although right now its empty, of
    # course, because we haven't done the processing yet)...
    output_dataset = analysis.output_manifest.get_dataset("cleaned_met_mast_data")

    # We'll add some labels, which will help to improve searchability and allow other apps, reports, users and
    # analyses to automatically find figures and use them.
    #
    # Get descriptive with labels... they are whitespace-delimited. Labels are case insensitive, and accept a-z, 0-9,
    # and hyphens which can be used literally in search and are also used to separate words in natural language search).
    # Other special characters will be stripped.
    output_dataset.labels = "met mast cleaned"

    # Create a Datafile to hold the concatenated, cleaned output data. We could put it in the current directory
    # (by leaving local_path_prefix unspecified) but it makes sense to put it in a folder specific to this output
    # dataset - doing so avoids any race conditions arising (if other instances of this application are running at the
    # same time), and avoids storage leaks, because files get cleaned up correctly.
    timeseries_datafile = Datafile(
        timestamp=None,
        path="cleaned.csv",
        skip_checks=True,  # We haven't created the actual file yet, so checks would definitely fail!
        labels="timeseries",
    )

    output_dataset.add(timeseries_datafile, path_in_dataset="cleaned.csv")

    # Write the file (now we know where to write it)
    with timeseries_datafile.open("w") as fp:
        data.to_csv(path_or_buf=fp)

    # And finally we add it to the output
    output_dataset.add(timeseries_datafile)

    # We're done! There's only one datafile in the output dataset, but you could create thousands more and append them
    # all :)
    #
    # If you're running this on your local machine, that's it - but when this code runs as an analysis in the cloud,
    # The files in the output manifest are copied into the cloud store. Their names and labels are registered in a search
    # index so your colleagues can find the dataset you've produced.
