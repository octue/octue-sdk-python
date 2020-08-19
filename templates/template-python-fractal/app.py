import os
import sys

from octue import analysis, octue_app
from .fractal import mandelbrot


@octue_app.command()
def run():
    """ Runs the application

    This is the function that gets run each time somebody requests an analysis from the digital twin / data service

    ## The Analysis:

    `analysis` is an instantiated Analysis class object, which you can import here (as shown) or anywhere else in your
    code. It contains:
        - The configuration values, which have been validated against the twine
        - The input values, which have been validated against the twine
        - The input manifest, which has been validated and whose files have been checked, instantiated as a
           Manifest object (files are checked to be present and, if a `sha` field is given in the manifest, their
           contents checked to match the sha)
        - An empty output_values dict which can be added to as required (on completion, will be validated
           against the twine)
        - An empty, instantiated, output Manifest object to which newly created datasets and files can be added (on
           completion, the presence of the files will be checked and their shas calculated)
        - Locations of configuration, input, tmp and output directories as strings

    """

    # You can use the general purpose logger to record debug statements, general information, warnings or errors
    analysis.logger.info(f"The input directory is {analysis.input_dir}")
    analysis.logger.info(f"The output directory is {analysis.output_dir}")
    analysis.logger.info(f"The tmp directory, where you can store temporary files or caches, is {analysis.tmp_dir}")

    # You can access any of the configuration or input values, anywhere in your code, from the analysis object
    analysis.logger.info(f"The maximum number of iterations will be {analysis.configuration['max_iterations']}")

    #
    mandelbrot()


@octue_app.command()
def version():
    """ Returns the version number of the application
    """

    # Top Tip:
    # For all Octue internal apps, we simply return the git revision of the code.
    # Every single commit creates a new version, we can always check out the exact version of the code that ran, and we
    # can quickly look up the version state and history on github when we have to debug an app. Sweet!
    version_no = os.system("git rev-parse HEAD")

    # Return the version number as a string
    return version_no


# If running from an IDE or test console, you won't be using the command line... just run this file
if __name__ == "__main__":

    # Manual setup
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "example_data"
    analysis.setup(id=None, data_dir=data_dir)
    run()
