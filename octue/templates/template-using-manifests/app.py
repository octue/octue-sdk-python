import logging
import os
import tempfile

from cleaner import clean, read_csv_files, read_dat_file

from octue.resources import Datafile, Dataset
from tests import TEST_BUCKET_NAME


logger = logging.getLogger(__name__)


def run(analysis):
    """Read a time series of files from a dataset, clean them, and write a new, cleaned dataset.

    See the "fractal" template for an introduction to the analysis object and the purpose of this 'run' function.

    Here, let's create an example application designed to clean up CSV data files produced by an instrument, in this
    case a meteorological mast.

    The aim of this example is to teach you how to use input and output file manifests in an app - so what we'll do is:
    - Use the input manifest to read a sequence of files
    - Perform a simple transformation on some of the data (as if we were doing a data cleaning process)
    - Create new files containing the cleaned data
    - Add them to the output manifest

    :param octue.resources.Analysis analysis:
    :return None:
    """
    # You can use a logger to record debug statements, general information, warnings or errors.
    logger.info("Starting clean up of files in %s", analysis.input_manifest)

    # Get the configuration value for our time averaging window (or if not present, use the default specified in
    # the twine).
    time_window = analysis.configuration_values.get("time_window", 600)
    logger.info("Averaging window set to %ss", time_window)

    # Get the input dataset which will be read in.
    input_dataset = analysis.input_manifest.get_dataset("raw_met_mast_data")

    # There are two types of files in the dataset. Metadata file(s), saved daily, and measurement files (saved hourly).
    # Because a manifest has been created, we're able to get this data out easily with the dataset filtering
    # capabilities. Let's get the metadata and the timeseries files, whilst showing off a couple of the filters.
    # See the Dataset class help for more.
    metadata_file = input_dataset.get_file_by_label("meta")

    timeseries_files = input_dataset.files.filter(labels__contains="timeseries").order_by(
        "tags__sequence",
        check_start_value=0,
        check_constant_increment=1,
    )

    # We used these because they're special helpers - in this case ensuring that there's only one metadata file and
    # ensuring that the timeseries files come in a strictly ordered sequence.
    #
    # We could also have picked up one or more files using general filters, like so:
    #
    #    metadata_files = input_dataset.files.filter(name__icontains="meta")
    #
    # There's generally a few ways to do it. Choose one which is likely to be most consistent - for example if your
    # filenames might be subject to change, but you have better control over the labels, rely on those.

    # At this point it's over to you, to do whatever you want with the contents of these files.
    # For this example app, we will:

    # Use a custom function to read in the strange metadata file that came with the dataset.
    metadata = read_dat_file(metadata_file)

    # Read the sequence of CSV files and concatenate into a pandas dataframe (like a table).
    data = read_csv_files(timeseries_files)

    # Clean the timeseries data up.
    data = clean(data, metadata["date"])

    # Create a temporary directory for the output dataset. This avoids any race conditions arising (if other instances
    # of this application are running at the same time) and avoids any data loss due to overwriting. The temporary
    # directory is deleted once the "with" block is exited.
    with tempfile.TemporaryDirectory() as temporary_directory:
        timeseries_datafile = Datafile(
            path=os.path.join(temporary_directory, "cleaned.csv"),
            labels=["timeseries"],
        )

        # Write the file (now we know where to write it)
        with timeseries_datafile.open("w") as fp:
            data.to_csv(path_or_buf=fp)

        # You can replace empty output datasets with datasets instantiated from a local or cloud directory.
        analysis.output_manifest.datasets["cleaned_met_mast_data"] = Dataset(
            path=temporary_directory,
            name="cleaned_met_mast_data",
        )

        # We'll add some labels, which will help to improve searchability and allow other apps, reports, users and
        # analyses to automatically find figures and use them.
        #
        # Labels are case insensitive, and accept a-z, 0-9, and hyphens which can be used literally in search and are
        # also used to separate words in natural language search.
        analysis.output_manifest.get_dataset("cleaned_met_mast_data").labels = ["met", "mast", "cleaned"]

        # Finalise the analysis. This validates the output data and output manifest against the twine and optionally
        # uploads any datasets in the output manifest to the service's cloud bucket. Signed URLs are provided so that
        # the parent that asked the service for the analysis can access the data (until the signed URLs expire).
        analysis.finalise(upload_output_datasets_to=f"gs://{TEST_BUCKET_NAME}/output/test_using_manifests_analysis")

    # We're done! There's only one datafile in the output dataset, but you could create thousands more and add them
    # all :)
    #
    # If you're running this on your local machine, that's it - but when this code runs as an analysis in the cloud,
    # the files in the output manifest are copied into the cloud store. Their names and labels can be registered in a
    # search index so your colleagues can find the dataset you've produced.
