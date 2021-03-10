from fractal import fractal


def run(analysis, *args, **kwargs):
    """Your main entrypoint to run the application

    This is the function that gets run each time somebody requests an analysis from the digital twin / data service.
    You should write your own code and call it from here.

    It needs to be called 'run' and the file must be called 'app.py'; Octue will handle the rest, supplying
    you with an "analysis" object with validated inputs for you to process.

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
    # analysis.logger.info("The input directory is %s", analysis.input_dir)
    # analysis.logger.info("The output directory is %s", analysis.output_dir)
    # analysis.logger.info("The tmp directory, where you can store temporary files or caches, is %s", analysis.tmp_dir)

    # Print statements will get logged...
    print("Hello! The app is running!")  # noqa: T001

    # ... but we encourage you to use the attached logger, which handles sending logs to remote services and allows them
    # to be viewed with twined server
    analysis.logger.info(
        "The logger can be used for capturing different 'levels' of statement - for debug, info, warnings or errors."
    )

    # You can access any of the configuration or input values, anywhere in your code, from the analysis object
    analysis.logger.info("The maximum number of iterations will be %s", analysis.configuration_values)

    # Run the code
    fractal(analysis)
