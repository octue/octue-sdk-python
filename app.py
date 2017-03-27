from oasys import oasys_app, oasys_analysis
import examples
import os


@oasys_app.command()
@oasys_analysis
def run(analysis):
    """ Runs the application on the oasys platform.

    :parameter analysis: an Analysis class object which contains the analysis configuration, the input file
    manifest and locations of input, tmp and output directories.

    """

    # EDIT THE CONTENTS OF THIS FUNCTION TO RUN WHATEVER ANALYSIS YOU WISH

    # SOME (VERY) SIMPLE EXAMPLES:

    # Print statements will get logged (stdout and stderr are mirrored to the log files so you don't miss anything)...
    print('Hello! The app is running!')

    # Do some processing, and add the results files
    file_name = analysis.output_dir + '/data_file_from_a_legacy_module'
    examples.a_legacy_analysis_function(file_name)

    # Create a figure using the results. This function adds to the output file manifest at the same time as creating the
    examples.create_figure_file()


@oasys_app.command()
def version():
    """ Returns the version number of the application
    """

    # Top Tip:
    # For all OASYS' internal apps, we simply return the git revision of the code.
    # Every single commit creates a new version, we can always check out the exact version of the code that ran, and we
    # can quickly look up the version state and history on github when we have to debug an app.
    version_no = os.system('git rev-parse HEAD')

    # Return the version number as a string
    return version_no
