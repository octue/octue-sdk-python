import os
import sys

from fractal import fractal
from octue import octue_cli, octue_run, octue_version


@octue_run
def run(analysis):
    """ Your main entrypoint to run the application

    This is the function that gets run each time somebody requests an analysis from the digital twin / data service.
    You should write your own code and call it from here.

    ## The Analysis:

    `analysis` is an instantiated Analysis class object, which you can import here (as shown) or anywhere else in your
    code. It contains:
        - ``configuration_values``, which have been validated against the twine
        - ``configuration_manifest``, a Manifest instance whose contents have been validated and whose files have been
           checked (files are checked to be present and, if a `sha` field is given in the manifest, their
           contents checked to match the sha)
        - ``input_values``, which have been validated against the twine
        - ``input_manifest``, a Manifest instance whose contents have been validated and whose files have been
           checked (files are checked to be present and, if a `sha` field is given in the manifest, their
           contents checked to match the sha)
        - ``output_values``, dict which can be added to as required (on completion, it will be validated
           against the twine and returned to the requester)
        - ``output_manifest``, a Manifest instance to which newly created datasets and files should be added (on
           completion, the presence of the files will be checked, their shas calculated and they'll be returned to the
           requester or uploaded)
        - ``children``, a dict of Child objects allowing you to access child twins/services
        - ``credentials``, a dict of Credential objects

    """

    # You can use the attached logger to record debug statements, general information, warnings or errors
    # analysis.logger.info(f"The input directory is {analysis.input_dir}")
    # analysis.logger.info(f"The output directory is {analysis.output_dir}")
    # analysis.logger.info(f"The tmp directory, where you can store temporary files or caches, is {analysis.tmp_dir}")

    # Print statements will get logged (stdout and stderr are mirrored to the log files so you don't miss anything)...
    print("Hello! The app is running!")  # noqa: T001

    # ... but we encourage you to use the attached logger, which handles sending logs to remote services and allows them
    # to be viewed with twined server
    analysis.logger.info(
        "The logger can be used for capturing different 'levels' of statement - for debug, info, warnings or errors."
    )

    # You can access any of the configuration or input values, anywhere in your code, from the analysis object
    analysis.logger.info(f"The maximum number of iterations will be {analysis.configuration_values}")

    # Run the code
    fractal(analysis)


@octue_version
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


# If running from an IDE or test console, it'll run this file rather than calling the application from the CLI...
# In that case we pass arguments through the CLI just as if it were called from the command line.
if __name__ == "__main__":

    # Invoke the CLI to process the arguments, set up an analysis and run it
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    octue_cli(args)
