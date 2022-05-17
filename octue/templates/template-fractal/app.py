import logging

from fractal import fractal


logger = logging.getLogger(__name__)


def run(analysis):
    """Your main entrypoint to run the application.

    This is the function that gets run each time somebody requests an analysis from the digital twin / data service.
    You should write your own code and call it from here.

    It needs to be called 'run' and the file must be called 'app.py'; Octue will handle the rest, supplying
    you with an "analysis" object with validated inputs for you to process.

    ## The Analysis:

    `analysis` is an instantiated Analysis class object, which you can use here (as shown) or anywhere else in your
    code. It contains:
        - ``configuration_values``, which have been validated against the twine
        - ``configuration_manifest``, a Manifest instance whose contents have been validated and whose datasets'
          presence has been checked
        - ``input_values``, which have been validated against the twine
        - ``input_manifest``, a Manifest instance whose contents have been validated and whose datasets'
          presence has been checked
        - ``output_values``, a dict which can be added to as required (on completion, it will be validated
           against the twine and returned to the parent)
        - ``output_manifest``, a Manifest instance to which newly created datasets and files should be added (on
           completion, the presence of the datasets will be checked and they'll be returned to the requester or uploaded)
        - ``children``, a dict of Child objects allowing you to access child twins/services
        - ``credentials``, a dict of Credential objects

    :param octue.resources.Analysis analysis:
    :return None:
    """
    # You can use a logger to record debug statements, general information, warnings or errors to forward to the parent.
    # logger.info("The input values are %s", analysis.input_values)

    # Print statements will get logged where the service is running...
    print("Hello! The app is running!")  # noqa: T001

    # ... but we encourage you to use the attached logger, which handles sending logs to parents and allows them
    # to be viewed with twined server.
    logger.info(
        "The logger can be used for capturing different 'levels' of statement - for debug, info, warnings or errors."
    )

    # You can access any of the configuration or input values, anywhere in your code, from the analysis object
    logger.info("The maximum number of iterations will be %s.", analysis.configuration_values["n_iterations"])

    # Create the fractal.
    data, layout = fractal(analysis)

    # Add the app's output values to the analysis.
    analysis.output_values = {"data": data, "layout": layout}

    # Finalise the analysis. This validates the output data and output manifest against the twine and optionally
    # uploads any datasets in the output manifest to the service's cloud bucket. Signed URLs are provided so that the
    # parent that asked the service for the analysis can access the data (until the signed URLs expire).
    analysis.finalise()
