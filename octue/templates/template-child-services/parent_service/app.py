import logging


logger = logging.getLogger(__name__)


def run(analysis):
    """Your main entrypoint to run the application

    See the "fractal" template for an introduction to the analysis object and the purpose of this 'run' function.

    :param octue.resources.Analysis analysis:
    :return None:
    """
    logger.info("Hello! The child services template app is running!")

    # Send input data to children specified in `app_configuration.json` and receive output data. The output comes as a
    # dictionary with an `output_values` key and an `output_manifest` key.
    elevations = analysis.children["elevation"].ask(input_values=analysis.input_values, timeout=60)["output_values"]
    wind_speeds = analysis.children["wind_speed"].ask(input_values=analysis.input_values, timeout=60)["output_values"]

    logger.info(
        "The wind speeds and elevations at %s are %s and %s.",
        analysis.input_values["locations"],
        wind_speeds,
        elevations,
    )

    # Do anything you like with the data you get from the children before setting the output values for your app's
    # analysis.
    analysis.output_values = {"wind_speeds": wind_speeds, "elevations": elevations}
    analysis.finalise()
